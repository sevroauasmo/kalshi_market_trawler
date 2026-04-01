"""Quick test: buy NO on second-favorite bucket across all cities."""
import sys
from datetime import timedelta
from trawler.backtesting.metar_backtest import KalshiClient, fetch_settled_events
from trawler.db.connection import get_connection
import logging
logging.basicConfig(level=logging.WARNING)

# Step 1: Pull events from API
print("Pulling events from Kalshi API...", flush=True)
client = KalshiClient()
all_events = {}
for s, c in [
    ("KXHIGHNY", "NYC"), ("KXHIGHCHI", "CHI"), ("KXHIGHMIA", "MIA"),
    ("KXHIGHAUS", "AUS"), ("KXHIGHDEN", "DEN"), ("KXHIGHLAX", "LA"),
]:
    evts = fetch_settled_events(client, s)
    all_events[s] = (c, evts)
    print(f"  {c}: {len(evts)} events", flush=True)
client.close()

# Step 2: Query DB
print("\nRunning backtest on early_price data...", flush=True)
conn = get_connection()
cur = conn.cursor()

grand_total = 0
grand_trades = 0
grand_wins = 0

for s, (city, evts) in all_events.items():
    if not evts:
        continue
    trades = []
    for e in evts:
        t = e["target_date"]
        cd = (t + timedelta(days=1)).strftime("%Y-%m-%d")
        cur.execute(
            "SELECT yes_sub_title, early_price::float, result "
            "FROM kalshi.historical_resolutions "
            "WHERE series_ticker = %s AND close_time::date = %s AND early_price IS NOT NULL "
            "ORDER BY early_price DESC",
            (s, cd),
        )
        rows = cur.fetchall()
        if len(rows) < 3:
            continue
        sub, ep, res = rows[1]  # second-favorite
        if ep is None or ep < 0.05 or ep > 0.50:
            continue
        nop = 1.0 - ep
        ano = 1 if res == "no" else 0
        pnl = ((1 - nop) / nop * 100) if ano == 1 else -100
        trades.append(pnl)

    if trades:
        w = sum(1 for p in trades if p > 0)
        tot = sum(trades)
        grand_total += tot
        grand_trades += len(trades)
        grand_wins += w
        print(
            f"  {city:<5} {len(trades):>3}t {w:>3}w ({w * 100 // len(trades)}%) "
            f"P&L: ${tot:>+7.0f}  ${tot / len(evts):>+.1f}/day",
            flush=True,
        )
    else:
        print(f"  {city:<5} no qualifying trades", flush=True)

conn.close()

if grand_trades:
    print(
        f"\n  TOTAL {grand_trades:>3}t {grand_wins:>3}w ({grand_wins * 100 // grand_trades}%) "
        f"P&L: ${grand_total:>+7.0f}",
        flush=True,
    )
