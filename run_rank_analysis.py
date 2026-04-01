"""Comprehensive rank-based analysis using early_price from DB.
Tests: which rank positions are most overpriced across all weather series?
Also tests daily LOW markets.
"""
from trawler.db.connection import get_connection
import sys

def analyze_series_group(series_list, label):
    """Analyze rank calibration and P&L for a group of series."""
    conn = get_connection()
    cur = conn.cursor()

    print(f"\n{'='*80}")
    print(f"{label}")
    print(f"{'='*80}")

    # First: calibration by rank across all series
    print(f"\nCalibration by rank (using early_price as entry):")
    print(f"{'Rank':<8} {'N':>5} {'AvgPrice':>9} {'WinRate':>8} {'Bias':>8} {'BuyNO P&L':>10}")

    for rank in range(6):
        cur.execute("""
            WITH ranked AS (
                SELECT series_ticker, close_time::date as dt,
                       yes_sub_title, early_price::float as ep, result,
                       ROW_NUMBER() OVER (
                           PARTITION BY series_ticker, close_time::date
                           ORDER BY early_price DESC
                       ) as rn
                FROM kalshi.historical_resolutions
                WHERE series_ticker = ANY(%s)
                  AND early_price IS NOT NULL AND result IS NOT NULL
            )
            SELECT ep, result FROM ranked WHERE rn = %s
        """, (series_list, rank + 1))
        rows = cur.fetchall()
        if not rows or len(rows) < 10:
            continue

        avg_price = sum(r[0] for r in rows) / len(rows)
        win_rate = sum(1 for r in rows if r[1] == 'yes') / len(rows)
        bias = win_rate - avg_price

        # P&L from buying NO
        pnl = 0
        trades = 0
        for ep, result in rows:
            if ep < 0.05 or ep > 0.50:
                continue
            no_price = 1.0 - ep
            if result == 'no':
                pnl += (ep / no_price) * 100
            else:
                pnl += -100
            trades += 1

        flag = ""
        if bias < -0.05:
            flag = " ← OVERPRICED"
        elif bias > 0.05:
            flag = " ← UNDERPRICED"

        print(f"rank_{rank:<3} {len(rows):>5} {avg_price:>8.1%} {win_rate:>7.1%} {bias:>+7.1%} "
              f"${pnl:>+9,.0f} ({trades}t){flag}")

    # Per-city breakdown for the most interesting rank
    print(f"\nPer-city P&L for rank_1 (second-favorite), buying NO on early_price:")
    for series in series_list:
        cur.execute("""
            WITH ranked AS (
                SELECT close_time::date as dt, yes_sub_title,
                       early_price::float as ep, result,
                       ROW_NUMBER() OVER (
                           PARTITION BY close_time::date ORDER BY early_price DESC
                       ) as rn
                FROM kalshi.historical_resolutions
                WHERE series_ticker = %s
                  AND early_price IS NOT NULL AND result IS NOT NULL
            )
            SELECT ep, result FROM ranked
            WHERE rn = 2 AND ep BETWEEN 0.05 AND 0.50
        """, (series,))
        rows = cur.fetchall()
        if not rows:
            continue

        wins = sum(1 for ep, r in rows if r == 'no')
        pnl = sum(
            (ep / (1 - ep) * 100) if r == 'no' else -100
            for ep, r in rows
        )
        avg_ep = sum(ep for ep, _ in rows) / len(rows)
        actual_yes = sum(1 for _, r in rows if r == 'yes') / len(rows)

        print(f"  {series:<20} {len(rows):>3}t {wins:>3}w ({wins*100//len(rows)}%) "
              f"P&L:${pnl:>+7,.0f}  avg_yes_price={avg_ep:.1%} actual_yes={actual_yes:.1%} "
              f"bias={actual_yes - avg_ep:+.1%}")

    conn.close()


# Daily HIGH markets (original 6 cities)
high_series = [
    'KXHIGHNY', 'KXHIGHCHI', 'KXHIGHMIA',
    'KXHIGHAUS', 'KXHIGHDEN', 'KXHIGHLAX',
]
analyze_series_group(high_series, "DAILY HIGH TEMPERATURE — Original 6 Cities")

# Daily HIGH markets (newer cities)
new_high_series = [
    'KXHIGHTATL', 'KXHIGHTBOS', 'KXHIGHTDC',
    'KXHIGHTPHX', 'KXHIGHTSEA', 'KXHIGHTLV',
    'KXHIGHTNOLA', 'KXHIGHTSFO', 'KXHIGHTMIN',
    'KXHIGHTDAL', 'KXHIGHTHOU', 'KXHIGHTOKC',
    'KXHIGHPHIL', 'KXHIGHTDEN', 'KXDENHIGH', 'KXDVHIGH',
]
analyze_series_group(new_high_series, "DAILY HIGH TEMPERATURE — Newer Cities")

# Daily LOW markets
low_series = [
    'KXLOWTNYC', 'KXLOWTCHI', 'KXLOWTMIA',
    'KXLOWTAUS', 'KXLOWTDEN', 'KXLOWTLAX',
    'KXLOWTPHIL', 'KXLOWNY', 'KXLOWCHI',
    'KXLOWMIA', 'KXLOWDEN', 'KXLOWLAX',
]
analyze_series_group(low_series, "DAILY LOW TEMPERATURE")

# All weather combined
all_weather = high_series + new_high_series + low_series
analyze_series_group(all_weather, "ALL WEATHER MARKETS COMBINED")
