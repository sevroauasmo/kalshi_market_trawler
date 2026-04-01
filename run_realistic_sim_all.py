"""
Realistic simulation across all 6 cities.
Same logic as NYC sim: buy NO on second-favorite at actual candlestick volumes/prices.
"""
import time as time_mod
import logging
from datetime import datetime, timedelta, timezone
from trawler.api.client import KalshiClient
from trawler.backtesting.metar_backtest import fetch_settled_events, parse_bucket

logging.basicConfig(level=logging.WARNING)

CITIES = [
    ("KXHIGHNY",  "NYC",     0.845, 0.35),
    ("KXHIGHCHI", "CHI",     0.719, 0.28),
    ("KXHIGHMIA", "MIA",     0.727, 0.30),
    ("KXHIGHAUS", "AUS",     0.775, 0.30),
    ("KXHIGHDEN", "DEN",     0.737, 0.28),
    ("KXHIGHLAX", "LA",      0.761, 0.30),
]

MAX_FILL_PCT = 0.30
MIN_YES = 0.08

client = KalshiClient()
all_summaries = []

for series, city, be_no, max_yes in CITIES:
    events = fetch_settled_events(client, series)
    if not events:
        continue

    print(f"\n{'='*70}", flush=True)
    print(f"{city} ({series}): {len(events)} events, BE_NO={be_no:.0%}, MAX_YES={max_yes:.0%}", flush=True)
    print(f"{'='*70}", flush=True)

    cum_pnl = 0
    peak = 0
    max_dd = 0
    total_deployed = 0
    trades = []

    for i, event in enumerate(events):
        target = event["target_date"]
        day_before = target - timedelta(days=1)

        # Identify second-favorite at 3pm ET
        id_time = day_before.replace(hour=19, minute=0, second=0, tzinfo=timezone.utc)
        bucket_means = []
        for mkt in event["markets"]:
            start_ts = int((id_time - timedelta(minutes=30)).timestamp())
            end_ts = int((id_time + timedelta(minutes=90)).timestamp())
            try:
                candles = client.get_candlesticks(series, mkt["ticker"],
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

        if second_mean < MIN_YES or second_mean > max_yes:
            continue

        # Get candlesticks 3pm-9pm ET
        ws = day_before.replace(hour=19, minute=0, second=0, tzinfo=timezone.utc)
        we = ws + timedelta(hours=8)
        try:
            candles = client.get_candlesticks(series, second_ticker,
                                               start_ts=int(ws.timestamp()),
                                               end_ts=int(we.timestamp()))
        except Exception:
            continue
        time_mod.sleep(0.1)

        total_contracts = 0
        total_cost = 0.0

        for c in candles:
            vol = float(c.get("volume_fp", "0"))
            if vol <= 0:
                continue
            yes_bid = float(c.get("yes_bid", {}).get("close_dollars", 0))
            yes_ask = float(c.get("yes_ask", {}).get("close_dollars", 0))
            if yes_bid <= 0:
                continue
            no_ask = 1.0 - yes_bid
            spread = yes_ask - yes_bid
            mean = float(c.get("price", {}).get("mean_dollars", 0))

            if no_ask >= be_no - 0.02:
                continue
            if mean < MIN_YES or mean > max_yes:
                continue

            available = vol * MAX_FILL_PCT
            ctrs = available / no_ask
            max_b4_impact = vol * 0.5
            ctrs = min(ctrs, max_b4_impact / no_ask)
            if ctrs < 1:
                continue

            impact = (ctrs * no_ask / max(vol, 1)) * spread
            fill = no_ask + impact / 2
            if fill >= be_no - 0.01:
                continue

            total_contracts += ctrs
            total_cost += ctrs * fill

        if total_contracts == 0:
            continue

        avg_fill = total_cost / total_contracts
        won = second_result == "no"
        pnl = total_contracts * (1.0 - avg_fill) if won else -total_cost

        total_deployed += total_cost
        cum_pnl += pnl
        peak = max(peak, cum_pnl)
        dd = peak - cum_pnl
        max_dd = max(max_dd, dd)

        trades.append({
            "date": target.strftime("%Y-%m-%d"), "bucket": second_sub,
            "yes": second_mean, "contracts": total_contracts,
            "cost": total_cost, "fill": avg_fill, "won": won,
            "pnl": pnl, "cum": cum_pnl, "dd": dd,
        })

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(events)}...", flush=True)

    if not trades:
        print(f"  No qualifying trades", flush=True)
        continue

    wins = sum(1 for t in trades if t["won"])
    print(f"\n  Trades: {len(trades)}, Wins: {wins} ({wins*100//len(trades)}%)", flush=True)
    print(f"  P&L: ${cum_pnl:+,.2f}, Deployed: ${total_deployed:,.2f}, ROI: {cum_pnl/total_deployed*100:.1f}%", flush=True)
    print(f"  Max DD: ${max_dd:,.2f} ({max_dd/peak*100:.0f}% from peak)" if peak > 0 else "", flush=True)
    print(f"  Avg position: ${total_deployed/len(trades):,.0f}, $/day: ${cum_pnl/len(events):+,.2f}", flush=True)

    # Monthly
    from collections import defaultdict
    monthly = defaultdict(lambda: {"pnl": 0, "t": 0, "w": 0, "d": 0})
    for t in trades:
        m = t["date"][:7]
        monthly[m]["pnl"] += t["pnl"]
        monthly[m]["t"] += 1
        monthly[m]["d"] += t["cost"]
        if t["won"]: monthly[m]["w"] += 1
    for m in sorted(monthly):
        d = monthly[m]
        print(f"    {m}: {d['t']}t {d['w']}w ({d['w']*100//d['t']}%) "
              f"P&L:${d['pnl']:+,.0f} ROI:{d['pnl']/d['d']*100:+.0f}%", flush=True)

    all_summaries.append({
        "city": city, "trades": len(trades), "wins": wins,
        "pnl": cum_pnl, "deployed": total_deployed, "max_dd": max_dd,
        "days": len(events), "peak": peak,
    })

client.close()

# Grand summary
print(f"\n{'='*80}", flush=True)
print("GRAND SUMMARY — Realistic Sim All Cities", flush=True)
print(f"{'='*80}", flush=True)
print(f"{'City':<6} {'Trades':>7} {'Wins':>5} {'Win%':>5} {'P&L':>10} "
      f"{'Deployed':>10} {'ROI':>6} {'MaxDD':>8} {'DD%':>5} {'$/day':>7}", flush=True)
print("-" * 80, flush=True)

gp = gd = gdd = 0
for s in sorted(all_summaries, key=lambda x: -x["pnl"]):
    wr = s["wins"]*100//s["trades"]
    roi = s["pnl"]/s["deployed"]*100 if s["deployed"] else 0
    ddp = s["max_dd"]/s["peak"]*100 if s["peak"] > 0 else 0
    ppd = s["pnl"]/s["days"]
    print(f"{s['city']:<6} {s['trades']:>7} {s['wins']:>5} {wr:>4}% ${s['pnl']:>+9,.0f} "
          f"${s['deployed']:>9,.0f} {roi:>5.1f}% ${s['max_dd']:>7,.0f} {ddp:>4.0f}% ${ppd:>+6.1f}",
          flush=True)
    gp += s["pnl"]
    gd += s["deployed"]
    gdd = max(gdd, s["max_dd"])

print("-" * 80, flush=True)
print(f"{'TOTAL':<6} {'':>7} {'':>5} {'':>5} ${gp:>+9,.0f} "
      f"${gd:>9,.0f} {gp/gd*100:>5.1f}%", flush=True)
print(f"\nCombined $/day: ${gp/98:+,.2f}", flush=True)
print(f"Annualized: ${gp/98*365:+,.0f}", flush=True)
