"""
Comprehensive Weather Market Analysis
=====================================
Tests rank-based structural mispricing across all weather market types.

Strategy: Buy YES on rank_1 (underpriced favorite) + Buy NO on rank_4 (overpriced 4th choice)
Entry: ~1hr after market open (early_price VWAP)
Slippage: 2c assumed on both legs

Uses DB early_price data only (fast, no API calls).
"""
from trawler.db.connection import get_connection
from collections import defaultdict
import sys

def run_analysis():
    conn = get_connection()
    cur = conn.cursor()

    # Discover all weather series with early_price data
    cur.execute("""
        SELECT series_ticker, COUNT(DISTINCT close_time::date) as days,
               COUNT(*) as markets
        FROM kalshi.historical_resolutions
        WHERE early_price IS NOT NULL AND result IS NOT NULL
          AND series_ticker LIKE 'KX%'
        GROUP BY series_ticker
        HAVING COUNT(DISTINCT close_time::date) >= 10
        ORDER BY COUNT(DISTINCT close_time::date) DESC
    """)
    series_with_data = [(r[0], r[1], r[2]) for r in cur.fetchall()]

    print(f"{'='*80}", flush=True)
    print(f"COMPREHENSIVE WEATHER RANK ANALYSIS", flush=True)
    print(f"Series with early_price data:", flush=True)
    for s, days, mkts in series_with_data:
        print(f"  {s:<25} {days:>4} days, {mkts:>5} markets", flush=True)

    # ─── 1. Rank Calibration per series ─────────────────────────────
    print(f"\n{'='*80}", flush=True)
    print(f"RANK CALIBRATION (bias = actual_win_rate - avg_market_price)", flush=True)
    print(f"{'Series':<25} {'R1 bias':>8} {'R2 bias':>8} {'R3 bias':>8} {'R4 bias':>8} {'R5 bias':>8}", flush=True)

    for series, days, mkts in series_with_data:
        biases = []
        for rank in range(1, 6):
            cur.execute("""
                WITH ranked AS (
                    SELECT close_time::date as dt, early_price::float as ep, result,
                           ROW_NUMBER() OVER (PARTITION BY close_time::date
                                              ORDER BY early_price DESC) as rn
                    FROM kalshi.historical_resolutions
                    WHERE series_ticker = %s AND early_price IS NOT NULL AND result IS NOT NULL
                )
                SELECT AVG(ep), AVG(CASE WHEN result='yes' THEN 1.0 ELSE 0.0 END), COUNT(*)
                FROM ranked WHERE rn = %s
            """, (series, rank))
            avg_p, act, n = cur.fetchone()
            if avg_p and n and n >= 5:
                biases.append(f"{float(act) - float(avg_p):>+7.1%}")
            else:
                biases.append(f"{'N/A':>8}")
        print(f"{series:<25} {'  '.join(biases)}", flush=True)

    # ─── 2. Combined Strategy (Buy YES r1 + Buy NO r4) per series ──
    print(f"\n{'='*80}", flush=True)
    print(f"COMBINED STRATEGY: Buy YES rank_1 + Buy NO rank_4 (2c slippage)", flush=True)
    print(f"{'Series':<25} {'Days':>5} {'P&L':>10} {'$/day':>8}", flush=True)

    grand_pnl = 0
    grand_days = 0

    for series, _, _ in series_with_data:
        cur.execute("""
            WITH ranked AS (
                SELECT close_time::date as dt, early_price::float as ep, result,
                       ROW_NUMBER() OVER (PARTITION BY close_time::date
                                          ORDER BY early_price DESC) as rn
                FROM kalshi.historical_resolutions
                WHERE series_ticker = %s AND early_price IS NOT NULL AND result IS NOT NULL
            )
            SELECT dt,
                   MAX(CASE WHEN rn=1 THEN ep END) as r1p,
                   MAX(CASE WHEN rn=1 THEN result END) as r1r,
                   MAX(CASE WHEN rn=4 THEN ep END) as r4p,
                   MAX(CASE WHEN rn=4 THEN result END) as r4r
            FROM ranked WHERE rn IN (1, 4)
            GROUP BY dt
            HAVING MAX(CASE WHEN rn=1 THEN ep END) BETWEEN 0.15 AND 0.60
               AND MAX(CASE WHEN rn=4 THEN ep END) BETWEEN 0.05 AND 0.25
        """, (series,))
        rows = cur.fetchall()
        if not rows:
            continue

        total_pnl = 0
        for dt, r1p, r1r, r4p, r4r in rows:
            # Leg 1: Buy YES on rank_1 at r1p + 2c
            yes_cost = r1p + 0.02
            pnl1 = ((1 - yes_cost) / yes_cost * 100) if r1r == "yes" else -100

            # Leg 2: Buy NO on rank_4 at (1-r4p) + 2c
            no_cost = (1 - r4p) + 0.02
            pnl2 = ((1 - no_cost) / no_cost * 100) if r4r == "no" else -100

            total_pnl += pnl1 + pnl2

        per_day = total_pnl / len(rows) if rows else 0
        marker = " ✓" if total_pnl > 0 else ""
        print(f"{series:<25} {len(rows):>5} ${total_pnl:>+9,.0f} ${per_day:>+7.1f}{marker}", flush=True)

        if total_pnl > 0:
            grand_pnl += total_pnl
            grand_days += len(rows)

    if grand_days:
        print(f"\n{'Profitable total':<25} {grand_days:>5} ${grand_pnl:>+9,.0f} ${grand_pnl/grand_days:>+7.1f}", flush=True)

    # ─── 3. Monthly Breakdown for profitable series ──────────────────
    print(f"\n{'='*80}", flush=True)
    print(f"MONTHLY BREAKDOWN (profitable series only, combined strategy)", flush=True)

    for month in [12, 1, 2, 3]:
        mname = {12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar"}[month]
        month_pnl = 0
        month_days = 0

        for series, _, _ in series_with_data:
            cur.execute("""
                WITH ranked AS (
                    SELECT close_time::date as dt,
                           EXTRACT(MONTH FROM close_time) as mon,
                           early_price::float as ep, result,
                           ROW_NUMBER() OVER (PARTITION BY close_time::date
                                              ORDER BY early_price DESC) as rn
                    FROM kalshi.historical_resolutions
                    WHERE series_ticker = %s AND early_price IS NOT NULL AND result IS NOT NULL
                )
                SELECT dt,
                       MAX(CASE WHEN rn=1 THEN ep END) as r1p,
                       MAX(CASE WHEN rn=1 THEN result END) as r1r,
                       MAX(CASE WHEN rn=4 THEN ep END) as r4p,
                       MAX(CASE WHEN rn=4 THEN result END) as r4r
                FROM ranked WHERE rn IN (1,4) AND mon = %s
                GROUP BY dt
                HAVING MAX(CASE WHEN rn=1 THEN ep END) BETWEEN 0.15 AND 0.60
                   AND MAX(CASE WHEN rn=4 THEN ep END) BETWEEN 0.05 AND 0.25
            """, (series, month))
            for dt, r1p, r1r, r4p, r4r in cur.fetchall():
                yes_cost = r1p + 0.02
                pnl1 = ((1 - yes_cost) / yes_cost * 100) if r1r == "yes" else -100
                no_cost = (1 - r4p) + 0.02
                pnl2 = ((1 - no_cost) / no_cost * 100) if r4r == "no" else -100
                month_pnl += pnl1 + pnl2
                month_days += 1

        if month_days:
            print(f"  {mname}: {month_days:>4} city-days  P&L: ${month_pnl:>+8,.0f}  "
                  f"${month_pnl/month_days:>+6.1f}/city-day", flush=True)

    # ─── 4. Rank_4 standalone (the more robust signal) ───────────────
    print(f"\n{'='*80}", flush=True)
    print(f"RANK_4 STANDALONE: Buy NO (most robust across cities)", flush=True)
    print(f"{'Series':<25} {'Trades':>7} {'Wins':>5} {'Win%':>5} {'VWAP P&L':>10} {'+2c P&L':>10} {'+3c P&L':>10}", flush=True)

    for series, _, _ in series_with_data:
        cur.execute("""
            WITH ranked AS (
                SELECT close_time::date as dt, early_price::float as ep, result,
                       ROW_NUMBER() OVER (PARTITION BY close_time::date
                                          ORDER BY early_price DESC) as rn
                FROM kalshi.historical_resolutions
                WHERE series_ticker = %s AND early_price IS NOT NULL AND result IS NOT NULL
            )
            SELECT ep, result FROM ranked WHERE rn = 4 AND ep BETWEEN 0.03 AND 0.30
        """, (series,))
        rows = cur.fetchall()
        if not rows or len(rows) < 10:
            continue

        wins = sum(1 for ep, r in rows if r == "no")
        pnl_vwap = sum((ep / (1 - ep) * 100) if r == "no" else -100 for ep, r in rows)
        pnl_2c = sum(
            ((ep - 0.02) / ((1 - ep) + 0.02) * 100) if r == "no" else -100
            for ep, r in rows
        )
        pnl_3c = sum(
            ((ep - 0.03) / ((1 - ep) + 0.03) * 100) if r == "no" else -100
            for ep, r in rows
        )
        wr = wins * 100 // len(rows)
        marker = " ✓" if pnl_2c > 0 else ""
        print(f"{series:<25} {len(rows):>7} {wins:>5} {wr:>4}% ${pnl_vwap:>+9,.0f} ${pnl_2c:>+9,.0f} ${pnl_3c:>+9,.0f}{marker}", flush=True)

    conn.close()
    print(f"\n{'='*80}", flush=True)
    print("Analysis complete.", flush=True)


if __name__ == "__main__":
    run_analysis()
