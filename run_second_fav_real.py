"""Second-favorite short strategy — real NO ask prices at multiple entry times."""
import sys
import time as time_mod
from datetime import datetime, timedelta, timezone
from trawler.backtesting.metar_backtest import KalshiClient, fetch_settled_events, parse_bucket
import logging
logging.basicConfig(level=logging.WARNING)

CITIES = [
    ("KXHIGHNY", "NYC"),
    ("KXHIGHCHI", "CHI"),
    ("KXHIGHMIA", "MIA"),
    ("KXHIGHAUS", "AUS"),
    ("KXHIGHDEN", "DEN"),
    ("KXHIGHLAX", "LA"),
]

HOURS = [
    (14, "10am"),  # market open
    (15, "11am"),  # 1hr after open
    (17, "1pm"),
    (19, "3pm"),
    (21, "5pm"),
]

client = KalshiClient()

# Pull all events first
print("Pulling events...", flush=True)
all_events = {}
for series, city in CITIES:
    evts = fetch_settled_events(client, series)
    all_events[series] = (city, evts)
    print(f"  {city}: {len(evts)}", flush=True)

# For each city x hour, get actual NO ask on second-favorite and compute P&L
print("\nPulling candlestick prices (this takes a while)...", flush=True)

results = {}  # (city, hour_label) -> {trades, wins, pnl}

for series, (city, evts) in all_events.items():
    if not evts:
        continue

    for hour_utc, hour_label in HOURS:
        trades = []

        for event in evts:
            target = event["target_date"]
            day_before = target - timedelta(days=1)
            price_time = day_before.replace(
                hour=hour_utc, minute=0, second=0, tzinfo=timezone.utc
            )

            # Get prices for all buckets at this time
            bucket_prices = []
            for mkt in event["markets"]:
                sub = mkt["yes_sub_title"]
                start_ts = int((price_time - timedelta(minutes=30)).timestamp())
                end_ts = int((price_time + timedelta(minutes=90)).timestamp())
                try:
                    candles = client.get_candlesticks(
                        series, mkt["ticker"], start_ts=start_ts, end_ts=end_ts
                    )
                except Exception:
                    continue
                time_mod.sleep(0.08)

                for c in candles:
                    if float(c.get("volume_fp", "0")) > 0:
                        yes_bid = float(
                            c.get("yes_bid", {}).get("close_dollars", 0)
                        )
                        mean = float(
                            c.get("price", {}).get("mean_dollars", 0)
                        )
                        vol = float(c.get("volume_fp", 0))
                        if yes_bid > 0 and mean > 0.01:
                            no_ask = 1.0 - yes_bid
                            bucket_prices.append(
                                {
                                    "sub": sub,
                                    "mean": mean,
                                    "no_ask": no_ask,
                                    "result": mkt["result"],
                                    "vol": vol,
                                }
                            )
                        break

            if len(bucket_prices) < 3:
                continue

            # Sort by mean price descending — second is rank 1
            sorted_b = sorted(bucket_prices, key=lambda x: -x["mean"])
            second = sorted_b[1]

            # Only trade if second-fav YES price is 5-50c
            if second["mean"] < 0.05 or second["mean"] > 0.50:
                continue

            # Buy NO at the actual NO ask price
            no_ask = second["no_ask"]
            if no_ask <= 0.52 or no_ask >= 0.99:
                continue

            actual_no = 1 if second["result"] == "no" else 0
            pnl = ((1 - no_ask) / no_ask * 100) if actual_no == 1 else -100
            trades.append(pnl)

        key = (city, hour_label)
        if trades:
            w = sum(1 for p in trades if p > 0)
            tot = sum(trades)
            results[key] = {"trades": len(trades), "wins": w, "pnl": tot}

    print(f"  Done: {city}", flush=True)

client.close()

# Print grid
print(f"\n{'='*85}", flush=True)
print(
    "SECOND-FAVORITE SHORT — Real NO Ask Prices — $100 flat bets", flush=True
)
print(f"{'='*85}", flush=True)

header = f"{'City':<6}"
for _, label in HOURS:
    header += f" | {label:>12}"
header += " | Total"
print(header, flush=True)
print("-" * 85, flush=True)

city_totals = {}
hour_totals = {label: 0 for _, label in HOURS}

for series, (city, evts) in all_events.items():
    line = f"{city:<6}"
    city_sum = 0
    for _, label in HOURS:
        r = results.get((city, label))
        if r:
            wr = r["wins"] * 100 // r["trades"]
            line += f" | ${r['pnl']:>+6,.0f} {wr:>2}%"
            city_sum += r["pnl"]
            hour_totals[label] += r["pnl"]
        else:
            line += f" |          N/A"
    city_totals[city] = city_sum
    line += f" | ${city_sum:>+7,.0f}"
    print(line, flush=True)

print("-" * 85, flush=True)
total_line = f"{'TOTAL':<6}"
grand = 0
for _, label in HOURS:
    total_line += f" | ${hour_totals[label]:>+6,.0f}    "
    grand += hour_totals[label]
total_line += f" | ${grand:>+7,.0f}"
print(total_line, flush=True)
