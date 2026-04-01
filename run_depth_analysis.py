"""Analyze market depth for second-favorite short strategy.
Key question: how much can we actually buy before hitting break-even?
If the second-fav wins 16% of the time, break-even NO price = 84c.
So we can buy NO from ~72c up to 84c and still be +EV.
"""
from trawler.api.client import KalshiClient
from trawler.backtesting.metar_backtest import fetch_settled_events
from datetime import datetime, timedelta, timezone
import time
import logging
logging.basicConfig(level=logging.WARNING)

client = KalshiClient()
events = fetch_settled_events(client, "KXHIGHNY")

print("NYC Second-Favorite Depth Analysis", flush=True)
print("=" * 80, flush=True)

# For each of last 15 days, get the second-favorite's full hourly profile
for event in events[-15:]:
    target = event["target_date"]
    day_before = target - timedelta(days=1)

    # First pass: identify second-favorite from 11am prices
    id_time = day_before.replace(hour=15, minute=0, second=0, tzinfo=timezone.utc)
    bucket_means = []
    for mkt in event["markets"]:
        start_ts = int((id_time - timedelta(minutes=30)).timestamp())
        end_ts = int((id_time + timedelta(minutes=90)).timestamp())
        try:
            candles = client.get_candlesticks("KXHIGHNY", mkt["ticker"],
                                               start_ts=start_ts, end_ts=end_ts)
        except:
            continue
        time.sleep(0.08)
        for c in candles:
            if float(c.get("volume_fp", "0")) > 0:
                mean = float(c.get("price", {}).get("mean_dollars", 0))
                if mean > 0.01:
                    bucket_means.append((mkt["ticker"], mkt["yes_sub_title"],
                                         mean, mkt["result"]))
                break

    if len(bucket_means) < 3:
        continue

    sorted_b = sorted(bucket_means, key=lambda x: -x[2])
    second_ticker = sorted_b[1][0]
    second_sub = sorted_b[1][1]
    second_result = sorted_b[1][3]
    second_mean = sorted_b[1][2]

    # Second pass: get full day of candles for the second-favorite
    start_ts = int(day_before.replace(hour=14, tzinfo=timezone.utc).timestamp())
    end_ts = int((day_before + timedelta(days=1)).replace(hour=5, tzinfo=timezone.utc).timestamp())
    try:
        candles = client.get_candlesticks("KXHIGHNY", second_ticker,
                                           start_ts=start_ts, end_ts=end_ts)
    except:
        continue
    time.sleep(0.1)

    vol_candles = [c for c in candles if float(c.get("volume_fp", "0")) > 0]
    total_vol = sum(float(c.get("volume_fp", 0)) for c in vol_candles)

    won = "WIN" if second_result == "yes" else "lose"
    print(f"\n{target.strftime('%b %d')}: {second_sub} (2nd fav, "
          f"11am mean={second_mean:.0%}) [{won}]", flush=True)
    print(f"  Total volume day-before: ${total_vol:,.0f}", flush=True)
    print(f"  {'Hour':>6} {'Vol':>7} {'YesBid':>7} {'YesAsk':>7} "
          f"{'NOask':>7} {'Spread':>7} {'OI':>7}", flush=True)

    for c in vol_candles:
        ts = c.get("end_period_ts", 0)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        hr_et = (dt.hour - 4) % 24
        vol = float(c.get("volume_fp", 0))
        yes_bid = float(c.get("yes_bid", {}).get("close_dollars", 0))
        yes_ask = float(c.get("yes_ask", {}).get("close_dollars", 0))
        oi = float(c.get("open_interest_fp", 0))
        no_ask = 1 - yes_bid if yes_bid > 0 else 0
        spread = yes_ask - yes_bid

        print(f"  {hr_et:>4}:00 ${vol:>6.0f} {yes_bid:>6.2f}c {yes_ask:>6.2f}c "
              f"{no_ask:>6.2f}c {spread:>6.2f}c ${oi:>6.0f}", flush=True)

    # Summary: if break-even NO = 84c (16% YES win rate),
    # how much could we buy between current NO ask and 84c?
    # The volume at each price level tells us
    if vol_candles:
        # Estimate: volume in the 3pm-8pm window (our target trading window)
        window_vol = sum(
            float(c.get("volume_fp", 0)) for c in vol_candles
            if 19 <= c.get("end_period_ts", 0) % 86400 // 3600 <= 24
            or c.get("end_period_ts", 0) % 86400 // 3600 < 1
        )
        # Actually just sum afternoon candles
        afternoon = [c for c in vol_candles
                     if datetime.fromtimestamp(c.get("end_period_ts", 0),
                                              tz=timezone.utc).hour >= 19]
        afternoon_vol = sum(float(c.get("volume_fp", 0)) for c in afternoon)
        print(f"  Afternoon (3pm+ ET) volume: ${afternoon_vol:,.0f}", flush=True)

client.close()
print("\nDone.", flush=True)
