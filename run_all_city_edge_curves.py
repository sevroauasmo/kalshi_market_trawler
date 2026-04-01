"""
Compute edge curves for ALL cities and determine optimal buy ranges.
For each city: at what NO price does the edge go to zero?
"""
from trawler.db.connection import get_connection

conn = get_connection()
cur = conn.cursor()

CITIES = [
    ("KXHIGHNY", "NYC"),
    ("KXHIGHCHI", "CHI"),
    ("KXHIGHMIA", "MIA"),
    ("KXHIGHAUS", "AUS"),
    ("KXHIGHDEN", "DEN"),
    ("KXHIGHLAX", "LA"),
]

BINS = [
    (0.05, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 0.25),
    (0.25, 0.30), (0.30, 0.35), (0.35, 0.40), (0.40, 0.50),
]

print("=" * 90, flush=True)
print("EDGE CURVES BY CITY — Second-Favorite (rank_2)", flush=True)
print("YES price = what the market charges for YES on second-fav", flush=True)
print("NO price = 1 - YES = what we'd pay for NO (approximate)", flush=True)
print("EV/$ = expected profit per dollar of NO purchased at that level", flush=True)
print("=" * 90, flush=True)

summary = []

for series, city in CITIES:
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

    if not rows or len(rows) < 20:
        print(f"\n{city}: insufficient data ({len(rows) if rows else 0} obs)", flush=True)
        continue

    total_yes = sum(1 for _, r in rows if r == "yes")
    overall_rate = total_yes / len(rows)
    be_no = 1 - overall_rate

    print(f"\n{'─'*70}", flush=True)
    print(f"{city} ({series}): {len(rows)} days, YES wins {overall_rate:.1%}, "
          f"break-even NO = {be_no:.0%}", flush=True)
    print(f"{'YES bin':<14} {'N':>4} {'YES wins':>9} {'YES%':>6} {'NO price':>9} "
          f"{'EV/$NO':>7} {'BE NO':>7} {'Trade?':>7}", flush=True)

    best_ev = 0
    profitable_range = None

    for lo, hi in BINS:
        subset = [(p, r) for p, r in rows if lo <= p < hi]
        if len(subset) < 3:
            continue
        n = len(subset)
        yes_wins = sum(1 for _, r in subset if r == "yes")
        yes_rate = yes_wins / n
        avg_yes = sum(p for p, _ in subset) / n
        avg_no = 1 - avg_yes
        ev = (1 - yes_rate) * (1 - avg_no) - yes_rate * avg_no
        bin_be = 1 - yes_rate

        trade = "YES" if ev > 0.01 else ("maybe" if ev > 0 else "NO")

        print(f"{lo:.0%}-{hi:.0%}         {n:>4} {yes_wins:>9} {yes_rate:>5.0%} "
              f"{avg_no:>8.0%} {ev:>6.3f} {bin_be:>6.0%} {trade:>7}", flush=True)

        if ev > 0.01:
            if profitable_range is None:
                profitable_range = (avg_no, avg_no)
            else:
                profitable_range = (min(profitable_range[0], avg_no),
                                     max(profitable_range[1], avg_no))
            best_ev = max(best_ev, ev)

    # Slippage analysis: how much does 1c, 2c, 3c of slippage cost?
    print(f"\n  Slippage sensitivity (all profitable bins combined):", flush=True)
    profitable_rows = [(p, r) for p, r in rows if 0.08 <= p <= 0.35]
    if profitable_rows:
        for slip in [0, 1, 2, 3, 4, 5]:
            slip_c = slip / 100
            pnl = 0
            trades = 0
            for yes_p, result in profitable_rows:
                no_price = (1 - yes_p) + slip_c  # add slippage to NO cost
                if no_price >= be_no:
                    continue  # would be past break-even
                trades += 1
                if result == "no":
                    pnl += ((1 - no_price) / no_price) * 100
                else:
                    pnl += -100
            if trades:
                print(f"    +{slip}c: {trades} trades, P&L: ${pnl:+,.0f} "
                      f"(${pnl/trades:+.1f}/trade)", flush=True)

    city_data = {
        "city": city, "series": series, "n": len(rows),
        "yes_rate": overall_rate, "be_no": be_no, "best_ev": best_ev,
    }
    if profitable_range:
        city_data["buy_no_from"] = profitable_range[0]
        city_data["buy_no_to"] = be_no - 0.02  # 2c margin
        print(f"\n  → Buy NO from {profitable_range[0]:.0%} up to {be_no-0.02:.0%} "
              f"(2c margin to BE)", flush=True)
    summary.append(city_data)

# Final summary
print(f"\n{'='*90}", flush=True)
print("DEPLOYMENT SUMMARY", flush=True)
print(f"{'='*90}", flush=True)
print(f"{'City':<6} {'N':>4} {'YES%':>6} {'BE NO':>7} {'Buy From':>9} {'Buy To':>8} "
      f"{'Best EV/$':>9} {'Viable?':>8}", flush=True)

for s in summary:
    viable = "YES" if s["best_ev"] > 0.02 else ("maybe" if s["best_ev"] > 0 else "NO")
    buy_from = f"{s.get('buy_no_from', 0):.0%}" if s.get("buy_no_from") else "N/A"
    buy_to = f"{s.get('buy_no_to', 0):.0%}" if s.get("buy_no_to") else "N/A"
    print(f"{s['city']:<6} {s['n']:>4} {s['yes_rate']:>5.1%} {s['be_no']:>6.0%} "
          f"{buy_from:>9} {buy_to:>8} {s['best_ev']:>8.3f} {viable:>8}", flush=True)

conn.close()
print("\nDone.", flush=True)
