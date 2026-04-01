"""
Realistic simulation: buy NO on second-favorite at ACTUAL available quantities
and prices from candlestick data. Track daily P&L, drawdown, ROI.

For each day:
1. Identify the second-favorite bucket at 3pm ET
2. Check if YES price is in our profitable range (8-35c)
3. Look at candlestick volume from 3pm-9pm ET
4. Buy NO at the actual bid levels shown in candlesticks
5. Cap purchases at 30% of hourly volume to avoid impact
6. Track running P&L, max drawdown, capital deployed
"""
import time as time_mod
import logging
from datetime import datetime, timedelta, timezone
from trawler.api.client import KalshiClient
from trawler.backtesting.metar_backtest import fetch_settled_events, parse_bucket

logging.basicConfig(level=logging.WARNING)

client = KalshiClient()
events = fetch_settled_events(client, "KXHIGHNY")
print(f"NYC: {len(events)} settled events", flush=True)

# For each event, get candlestick data for the second-favorite
# across 3pm-9pm ET window (19:00-01:00 UTC)
BE_NO = 0.845  # break-even from edge curve (15.5% YES rate)
MAX_YES = 0.35
MIN_YES = 0.08
MAX_FILL_PCT = 0.30  # buy max 30% of hourly volume

daily_results = []
cumulative_pnl = 0
peak_pnl = 0
max_drawdown = 0
total_deployed = 0
total_returned = 0

for i, event in enumerate(events):
    target = event["target_date"]
    day_before = target - timedelta(days=1)

    # Step 1: Get all bucket prices at 3pm ET to identify second-favorite
    id_time = day_before.replace(hour=19, minute=0, second=0, tzinfo=timezone.utc)
    bucket_means = []
    for mkt in event["markets"]:
        start_ts = int((id_time - timedelta(minutes=30)).timestamp())
        end_ts = int((id_time + timedelta(minutes=90)).timestamp())
        try:
            candles = client.get_candlesticks("KXHIGHNY", mkt["ticker"],
                                               start_ts=start_ts, end_ts=end_ts)
        except Exception:
            continue
        time_mod.sleep(0.08)
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
    second_mean = sorted_b[1][2]
    second_result = sorted_b[1][3]

    if second_mean < MIN_YES or second_mean > MAX_YES:
        daily_results.append({
            "date": target.strftime("%Y-%m-%d"),
            "bucket": second_sub,
            "action": "SKIP (price)",
            "yes_price": second_mean,
            "contracts": 0, "cost": 0, "pnl": 0,
        })
        continue

    # Step 2: Get ALL candlesticks from 3pm-9pm ET for this bucket
    window_start = day_before.replace(hour=19, minute=0, second=0, tzinfo=timezone.utc)
    window_end = day_before.replace(hour=23, minute=59, second=0, tzinfo=timezone.utc) + timedelta(hours=2)
    start_ts = int(window_start.timestamp())
    end_ts = int(window_end.timestamp())

    try:
        candles = client.get_candlesticks("KXHIGHNY", second_ticker,
                                           start_ts=start_ts, end_ts=end_ts)
    except Exception:
        continue
    time_mod.sleep(0.1)

    # Step 3: Simulate buying NO at each hourly candle
    total_contracts = 0
    total_cost = 0.0
    fills = []

    for c in candles:
        vol = float(c.get("volume_fp", "0"))
        if vol <= 0:
            continue

        yes_bid = float(c.get("yes_bid", {}).get("close_dollars", 0))
        yes_ask = float(c.get("yes_ask", {}).get("close_dollars", 0))

        if yes_bid <= 0:
            continue

        no_ask = 1.0 - yes_bid  # what we pay to buy NO
        spread = yes_ask - yes_bid

        # Don't buy if NO price is above our break-even minus margin
        if no_ask >= BE_NO - 0.02:
            continue

        # Don't buy if YES price has moved outside our range
        mean = float(c.get("price", {}).get("mean_dollars", 0))
        if mean < MIN_YES or mean > MAX_YES:
            continue

        # Buy up to MAX_FILL_PCT of this hour's volume
        available_dollars = vol * MAX_FILL_PCT
        # Each NO contract costs no_ask dollars
        contracts_available = available_dollars / no_ask
        contracts_to_buy = contracts_available

        # Also: don't push price more than 2c
        # Rough model: buying X% of volume moves price by X% * spread * 2
        max_before_impact = vol * 0.5  # very conservative
        contracts_to_buy = min(contracts_to_buy, max_before_impact / no_ask)

        # Floor at 1 contract
        if contracts_to_buy < 1:
            continue

        # Estimated fill price: no_ask + small impact
        impact = (contracts_to_buy * no_ask / max(vol, 1)) * spread
        fill_price = no_ask + impact / 2

        if fill_price >= BE_NO - 0.01:
            continue

        cost = contracts_to_buy * fill_price
        total_contracts += contracts_to_buy
        total_cost += cost

        ts = c.get("end_period_ts", 0)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        fills.append({
            "time": dt.strftime("%H:%M"),
            "contracts": contracts_to_buy,
            "fill": fill_price,
            "cost": cost,
            "vol": vol,
        })

    if total_contracts == 0:
        daily_results.append({
            "date": target.strftime("%Y-%m-%d"),
            "bucket": second_sub,
            "action": "SKIP (no fills)",
            "yes_price": second_mean,
            "contracts": 0, "cost": 0, "pnl": 0,
        })
        continue

    # Step 4: Compute P&L
    avg_fill = total_cost / total_contracts
    won = second_result == "no"

    if won:
        # Each contract pays $1, we paid avg_fill each
        pnl = total_contracts * (1.0 - avg_fill)
    else:
        # Lose everything we paid
        pnl = -total_cost

    total_deployed += total_cost
    total_returned += (total_contracts if won else 0)
    cumulative_pnl += pnl
    peak_pnl = max(peak_pnl, cumulative_pnl)
    drawdown = peak_pnl - cumulative_pnl
    max_drawdown = max(max_drawdown, drawdown)

    daily_results.append({
        "date": target.strftime("%Y-%m-%d"),
        "bucket": second_sub,
        "action": "TRADE",
        "yes_price": second_mean,
        "contracts": total_contracts,
        "cost": total_cost,
        "avg_fill": avg_fill,
        "won": won,
        "pnl": pnl,
        "cum_pnl": cumulative_pnl,
        "drawdown": drawdown,
        "fills": len(fills),
    })

    if (i + 1) % 10 == 0:
        print(f"  {i+1}/{len(events)}...", flush=True)

client.close()

# ─── RESULTS ──────────────────────────────────────────────────
trades = [d for d in daily_results if d["action"] == "TRADE"]
wins = [t for t in trades if t.get("won")]
losses = [t for t in trades if not t.get("won")]

print(f"\n{'='*80}", flush=True)
print("NYC REALISTIC SIMULATION — Second-Favorite Short", flush=True)
print(f"Entry window: 3pm-9pm ET day-before | Max fill: {MAX_FILL_PCT:.0%} of hourly vol", flush=True)
print(f"YES range: {MIN_YES:.0%}-{MAX_YES:.0%} | Break-even NO: {BE_NO:.0%}", flush=True)
print(f"{'='*80}", flush=True)

print(f"\nTrades: {len(trades)} / {len(events)} days", flush=True)
print(f"Wins: {len(wins)} ({len(wins)*100//len(trades) if trades else 0}%)", flush=True)
print(f"Losses: {len(losses)}", flush=True)

if trades:
    print(f"\nTotal P&L: ${cumulative_pnl:+,.2f}", flush=True)
    print(f"Total deployed: ${total_deployed:,.2f}", flush=True)
    print(f"Total returned: ${total_returned:,.2f}", flush=True)
    print(f"ROI: {cumulative_pnl/total_deployed*100:.1f}%", flush=True)
    print(f"\nPeak P&L: ${peak_pnl:,.2f}", flush=True)
    print(f"Max drawdown: ${max_drawdown:,.2f}", flush=True)
    print(f"Max drawdown from peak: {max_drawdown/peak_pnl*100:.0f}%" if peak_pnl > 0 else "", flush=True)

    avg_win = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
    avg_cost = sum(t["cost"] for t in trades) / len(trades)
    avg_contracts = sum(t["contracts"] for t in trades) / len(trades)

    print(f"\nAvg position size: ${avg_cost:,.2f} ({avg_contracts:.1f} contracts)", flush=True)
    print(f"Avg win: ${avg_win:+,.2f}", flush=True)
    print(f"Avg loss: ${avg_loss:+,.2f}", flush=True)
    print(f"Win/loss ratio: {abs(avg_win/avg_loss):.2f}x" if avg_loss else "", flush=True)
    print(f"$/day (all days): ${cumulative_pnl/len(events):+,.2f}", flush=True)
    print(f"$/trade: ${cumulative_pnl/len(trades):+,.2f}", flush=True)

# Individual trade log
print(f"\n{'Date':<11} {'Bucket':<14} {'YES%':>5} {'Ctrcts':>7} {'Cost':>8} "
      f"{'AvgFill':>7} {'W/L':>4} {'P&L':>9} {'CumP&L':>9} {'DD':>7}", flush=True)
print("-" * 95, flush=True)

for t in trades:
    w = "WIN" if t.get("won") else "LOSS"
    color_start = ""
    print(f"{t['date']:<11} {t['bucket']:<14} {t['yes_price']:>4.0%} "
          f"{t['contracts']:>7.1f} ${t['cost']:>7.2f} "
          f"{t.get('avg_fill',0):>6.2f}c {w:>4} "
          f"${t['pnl']:>+8.2f} ${t['cum_pnl']:>+8.2f} ${t.get('drawdown',0):>6.2f}",
          flush=True)

# Monthly breakdown
print(f"\nMonthly breakdown:", flush=True)
from collections import defaultdict
monthly = defaultdict(lambda: {"pnl": 0, "trades": 0, "wins": 0, "cost": 0})
for t in trades:
    m = t["date"][:7]
    monthly[m]["pnl"] += t["pnl"]
    monthly[m]["trades"] += 1
    monthly[m]["cost"] += t["cost"]
    if t.get("won"):
        monthly[m]["wins"] += 1

for m in sorted(monthly):
    d = monthly[m]
    wr = d["wins"]*100//d["trades"] if d["trades"] else 0
    roi = d["pnl"]/d["cost"]*100 if d["cost"] else 0
    print(f"  {m}: {d['trades']} trades, {d['wins']} wins ({wr}%), "
          f"P&L: ${d['pnl']:+,.2f}, deployed: ${d['cost']:,.2f}, ROI: {roi:+.1f}%",
          flush=True)
