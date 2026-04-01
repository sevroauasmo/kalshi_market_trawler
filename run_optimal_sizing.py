"""
Optimal position sizing and realistic P&L simulation for second-favorite short.

For each city:
1. Compute the actual YES win rate by price bin (edge curve)
2. Determine break-even NO price
3. Simulate buying NO from current ask up toward break-even, accounting for price impact
4. Compute realistic daily P&L with volume constraints

Then set up the forward test schema.
"""
from trawler.db.connection import get_connection
from trawler.api.client import KalshiClient
from trawler.backtesting.metar_backtest import fetch_settled_events
from datetime import datetime, timedelta, timezone
import time as time_mod
import logging
import json

logging.basicConfig(level=logging.WARNING)

CITIES = [
    ("KXHIGHNY", "NYC"),
    ("KXHIGHCHI", "CHI"),
    ("KXHIGHMIA", "MIA"),
    ("KXHIGHAUS", "AUS"),
    ("KXHIGHDEN", "DEN"),
    ("KXHIGHLAX", "LA"),
]


def compute_edge_curve(series):
    """Compute YES win rate by price bin for the second-favorite."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH ranked AS (
            SELECT close_time::date as dt, early_price::float as yes_price, result,
                   ROW_NUMBER() OVER (PARTITION BY close_time::date
                                      ORDER BY early_price DESC) as rn
            FROM kalshi.historical_resolutions
            WHERE series_ticker = %s AND early_price IS NOT NULL AND result IS NOT NULL
        )
        SELECT yes_price, result FROM ranked WHERE rn = 2
        ORDER BY yes_price
    """, (series,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return None

    total_yes = sum(1 for _, r in rows if r == "yes")
    overall_rate = total_yes / len(rows)
    be_no = 1 - overall_rate

    # Bin analysis
    bins = []
    for lo, hi in [(0.05, 0.15), (0.15, 0.20), (0.20, 0.25), (0.25, 0.30),
                   (0.30, 0.35), (0.35, 0.40), (0.40, 0.50)]:
        subset = [(p, r) for p, r in rows if lo <= p < hi]
        if len(subset) < 3:
            continue
        n = len(subset)
        yes_wins = sum(1 for _, r in subset if r == "yes")
        yes_rate = yes_wins / n
        avg_yes = sum(p for p, _ in subset) / n
        avg_no = 1 - avg_yes
        ev = (1 - yes_rate) * (1 - avg_no) - yes_rate * avg_no
        bins.append({
            "lo": lo, "hi": hi, "n": n, "yes_rate": yes_rate,
            "avg_no": avg_no, "ev_per_dollar": ev, "be_no": 1 - yes_rate,
        })

    return {
        "total": len(rows),
        "overall_yes_rate": overall_rate,
        "break_even_no": be_no,
        "bins": bins,
    }


def simulate_realistic_pnl(series, city, edge_curve):
    """Simulate realistic P&L using candlestick data for volume/price constraints."""
    client = KalshiClient()
    events = fetch_settled_events(client, series)

    be_no = edge_curve["break_even_no"]
    # Only trade when YES price is in profitable range
    max_yes = 0.35  # don't short if second-fav > 35c (too competitive)
    min_yes = 0.08  # too cheap to bother

    results = []

    for i, event in enumerate(events):
        target = event["target_date"]
        day_before = target - timedelta(days=1)

        # Get all bucket prices at 3pm ET (19:00 UTC)
        price_time = day_before.replace(hour=19, minute=0, second=0, tzinfo=timezone.utc)

        bucket_data = []
        for mkt in event["markets"]:
            start_ts = int((price_time - timedelta(minutes=30)).timestamp())
            end_ts = int((price_time + timedelta(minutes=90)).timestamp())
            try:
                candles = client.get_candlesticks(series, mkt["ticker"],
                                                   start_ts=start_ts, end_ts=end_ts)
            except Exception:
                continue
            time_mod.sleep(0.08)

            for c in candles:
                if float(c.get("volume_fp", "0")) > 0:
                    mean = float(c.get("price", {}).get("mean_dollars", 0))
                    yes_bid = float(c.get("yes_bid", {}).get("close_dollars", 0))
                    yes_ask = float(c.get("yes_ask", {}).get("close_dollars", 0))
                    vol = float(c.get("volume_fp", 0))
                    oi = float(c.get("open_interest_fp", 0))
                    if mean > 0.01 and yes_bid > 0:
                        bucket_data.append({
                            "sub": mkt["yes_sub_title"],
                            "mean": mean, "yes_bid": yes_bid, "yes_ask": yes_ask,
                            "vol": vol, "oi": oi, "result": mkt["result"],
                        })
                    break

        if len(bucket_data) < 3:
            continue

        # Find second-favorite
        sorted_b = sorted(bucket_data, key=lambda x: -x["mean"])
        second = sorted_b[1]

        yes_price = second["mean"]
        if yes_price < min_yes or yes_price > max_yes:
            results.append({
                "date": target.strftime("%Y-%m-%d"),
                "bucket": second["sub"],
                "yes_price": yes_price,
                "action": "SKIP (price out of range)",
                "position": 0, "pnl": 0,
            })
            continue

        no_ask = 1 - second["yes_bid"]  # what we actually pay
        spread = second["yes_ask"] - second["yes_bid"]
        hourly_vol = second["vol"]

        # Position sizing:
        # We can buy up to ~50% of hourly volume without major impact
        # Each $1 moves price by roughly spread/hourly_vol
        # We want to buy until NO price hits (be_no - 2c margin)
        max_no = be_no - 0.02  # leave 2c margin to break-even
        room = max_no - no_ask  # how much price room we have

        if room <= 0.01:
            results.append({
                "date": target.strftime("%Y-%m-%d"),
                "bucket": second["sub"],
                "yes_price": yes_price,
                "action": "SKIP (no room to break-even)",
                "position": 0, "pnl": 0,
            })
            continue

        # Estimate position size: buy 30% of hourly volume
        # Assume this moves price by ~1-2c
        position = min(hourly_vol * 0.30, 500)  # cap at $500
        position = max(position, 50)  # minimum $50

        # Estimated fill price: midpoint between ask and our impact
        # Assume we move price by position/hourly_vol * spread * 2
        impact = (position / max(hourly_vol, 1)) * spread * 2
        fill_no = no_ask + impact / 2  # average fill halfway through impact

        if fill_no >= max_no:
            # Would push past break-even, size down
            position = hourly_vol * 0.15
            fill_no = no_ask + (position / max(hourly_vol, 1)) * spread
            if fill_no >= max_no:
                results.append({
                    "date": target.strftime("%Y-%m-%d"),
                    "bucket": second["sub"],
                    "yes_price": yes_price,
                    "action": "SKIP (impact too high)",
                    "position": 0, "pnl": 0,
                })
                continue

        # Compute P&L
        actual_no = 1 if second["result"] == "no" else 0
        if actual_no == 1:
            # Win: collect $1 per contract, paid fill_no each
            # Number of contracts = position / fill_no (since each costs fill_no)
            n_contracts = position / fill_no
            pnl = n_contracts * (1 - fill_no)  # profit per contract = 1 - cost
        else:
            # Lose: paid position dollars, get nothing
            pnl = -position

        results.append({
            "date": target.strftime("%Y-%m-%d"),
            "bucket": second["sub"],
            "yes_price": round(yes_price, 3),
            "no_ask": round(no_ask, 3),
            "fill_no": round(fill_no, 3),
            "hourly_vol": round(hourly_vol),
            "position": round(position),
            "result": second["result"],
            "pnl": round(pnl, 2),
            "action": "TRADE",
        })

        if (i + 1) % 20 == 0:
            print(f"  {city} {i+1}/{len(events)}...", flush=True)

    client.close()
    return results


# ─── MAIN ────────────────────────────────────────────────────────
print("=" * 80, flush=True)
print("OPTIMAL SIZING & REALISTIC P&L SIMULATION", flush=True)
print("=" * 80, flush=True)

all_city_results = {}

for series, city in CITIES:
    print(f"\n{'─'*60}", flush=True)
    print(f"{city} ({series})", flush=True)
    print(f"{'─'*60}", flush=True)

    # Step 1: Edge curve
    ec = compute_edge_curve(series)
    if not ec:
        print("  No data", flush=True)
        continue

    print(f"  Overall: {ec['total']} days, YES wins {ec['overall_yes_rate']:.1%}, "
          f"break-even NO = {ec['break_even_no']:.0%}", flush=True)
    print(f"  {'YES bin':<12} {'N':>4} {'YES%':>6} {'NO price':>9} {'EV/$':>7} {'BE NO':>7}", flush=True)
    for b in ec["bins"]:
        profitable = "+" if b["ev_per_dollar"] > 0 else "-"
        print(f"  {b['lo']:.0%}-{b['hi']:.0%}       {b['n']:>4} {b['yes_rate']:>5.0%} "
              f"{b['avg_no']:>8.0%} {b['ev_per_dollar']:>6.3f} {b['be_no']:>6.0%} {profitable}",
              flush=True)

    # Step 2: Realistic P&L simulation
    print(f"\n  Simulating with price impact...", flush=True)
    results = simulate_realistic_pnl(series, city, ec)

    trades = [r for r in results if r["action"] == "TRADE"]
    skips = [r for r in results if r["action"] != "TRADE"]

    if not trades:
        print("  No qualifying trades", flush=True)
        continue

    wins = sum(1 for t in trades if t["pnl"] > 0)
    total_pnl = sum(t["pnl"] for t in trades)
    total_deployed = sum(t["position"] for t in trades)
    avg_position = total_deployed / len(trades)
    avg_fill = sum(t.get("fill_no", 0) for t in trades) / len(trades)

    print(f"  Trades: {len(trades)} (skipped: {len(skips)})", flush=True)
    print(f"  Wins: {wins} ({wins*100//len(trades)}%)", flush=True)
    print(f"  Total P&L: ${total_pnl:+,.0f}", flush=True)
    print(f"  Total deployed: ${total_deployed:,.0f}", flush=True)
    print(f"  Avg position: ${avg_position:.0f}", flush=True)
    print(f"  Avg NO fill: {avg_fill:.2f}", flush=True)
    print(f"  ROI: {total_pnl/total_deployed*100:.1f}%", flush=True)
    n_days = len(set(r["date"] for r in results))
    print(f"  $/day: ${total_pnl/n_days:+.1f}", flush=True)

    all_city_results[city] = {
        "trades": len(trades), "wins": wins, "pnl": total_pnl,
        "deployed": total_deployed, "avg_pos": avg_position,
        "days": n_days, "edge_curve": ec,
    }

    # Show individual trades
    print(f"\n  {'Date':<11} {'Bucket':<14} {'YES':>5} {'NOask':>6} {'Fill':>6} "
          f"{'Vol':>6} {'Size':>6} {'Result':>6} {'P&L':>8}", flush=True)
    for t in trades[-20:]:
        print(f"  {t['date']:<11} {t['bucket']:<14} {t['yes_price']:>4.0%} "
              f"{t.get('no_ask',0):>5.0%} {t.get('fill_no',0):>5.0%} "
              f"${t.get('hourly_vol',0):>5} ${t['position']:>5} "
              f"{t['result']:>5} ${t['pnl']:>+7.0f}", flush=True)

# ─── SUMMARY ─────────────────────────────────────────────────────
print(f"\n{'='*80}", flush=True)
print("SUMMARY — Realistic Position Sizing", flush=True)
print(f"{'='*80}", flush=True)
print(f"{'City':<6} {'Trades':>7} {'Wins':>5} {'Win%':>5} {'P&L':>9} "
      f"{'Deployed':>9} {'ROI':>6} {'$/day':>7} {'BE NO':>6}", flush=True)

grand_pnl = 0
grand_deployed = 0

for city, r in sorted(all_city_results.items(), key=lambda x: -x[1]["pnl"]):
    wr = r["wins"] * 100 // r["trades"] if r["trades"] else 0
    roi = r["pnl"] / r["deployed"] * 100 if r["deployed"] else 0
    per_day = r["pnl"] / r["days"] if r["days"] else 0
    be = r["edge_curve"]["break_even_no"]
    print(f"{city:<6} {r['trades']:>7} {r['wins']:>5} {wr:>4}% ${r['pnl']:>+8,.0f} "
          f"${r['deployed']:>8,.0f} {roi:>5.1f}% ${per_day:>+6.1f} {be:>5.0%}", flush=True)
    grand_pnl += r["pnl"]
    grand_deployed += r["deployed"]

print(f"\n{'TOTAL':<6} {'':>7} {'':>5} {'':>5} ${grand_pnl:>+8,.0f} "
      f"${grand_deployed:>8,.0f} {grand_pnl/grand_deployed*100 if grand_deployed else 0:>5.1f}%",
      flush=True)
