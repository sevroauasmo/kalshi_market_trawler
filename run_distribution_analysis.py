"""Analyze: how concentrated vs spread is the market's probability distribution?
If the market concentrates too much on favorites, buying the field is +EV.
If the market spreads too much, buying the favorite is +EV.
"""
from trawler.db.connection import get_connection

conn = get_connection()
cur = conn.cursor()

SERIES_GROUPS = {
    "HIGH temp (original 6)": [
        'KXHIGHNY', 'KXHIGHCHI', 'KXHIGHMIA',
        'KXHIGHAUS', 'KXHIGHDEN', 'KXHIGHLAX',
    ],
    "LOW temp": [
        'KXLOWTNYC', 'KXLOWTCHI', 'KXLOWTMIA',
        'KXLOWTAUS', 'KXLOWTDEN', 'KXLOWTLAX',
        'KXLOWTPHIL',
    ],
    "HIGH temp (newer)": [
        'KXHIGHTATL', 'KXHIGHTBOS', 'KXHIGHTDC',
        'KXHIGHTPHX', 'KXHIGHTSEA', 'KXHIGHTLV',
        'KXHIGHTNOLA', 'KXHIGHTSFO',
    ],
}

for group_name, series_list in SERIES_GROUPS.items():
    print(f"\n{'='*70}")
    print(f"{group_name}")
    print(f"{'='*70}")

    # For each event-day, rank buckets by early_price and check where the winner falls
    cur.execute("""
        WITH ranked AS (
            SELECT series_ticker, close_time::date as dt,
                   yes_sub_title, early_price::float as ep, result,
                   ROW_NUMBER() OVER (
                       PARTITION BY series_ticker, close_time::date
                       ORDER BY early_price DESC
                   ) as rn,
                   COUNT(*) OVER (
                       PARTITION BY series_ticker, close_time::date
                   ) as total_buckets
            FROM kalshi.historical_resolutions
            WHERE series_ticker = ANY(%s)
              AND early_price IS NOT NULL AND result IS NOT NULL
        )
        SELECT dt, series_ticker, rn as winner_rank, ep as winner_price, total_buckets
        FROM ranked
        WHERE result = 'yes'
        ORDER BY dt
    """, (series_list,))
    rows = cur.fetchall()

    if not rows:
        print("  No data")
        continue

    print(f"  Total event-days with data: {len(rows)}")

    # Distribution of winner rank
    from collections import Counter
    rank_dist = Counter(r[2] for r in rows)
    print(f"\n  Where does the winner fall in the market's ranking?")
    for rank in sorted(rank_dist.keys()):
        count = rank_dist[rank]
        pct = count / len(rows) * 100
        bar = "█" * int(pct / 2)
        print(f"    Rank {rank}: {count:>4} ({pct:>5.1f}%) {bar}")

    # Cumulative: what % of the time is the winner in top N?
    print(f"\n  Cumulative:")
    cumulative = 0
    for rank in range(1, max(rank_dist.keys()) + 1):
        cumulative += rank_dist.get(rank, 0)
        pct = cumulative / len(rows) * 100
        print(f"    Top {rank}: {pct:>5.1f}%")

    # Average winner price (what the market thought the winner's chance was)
    avg_winner_price = sum(r[3] for r in rows) / len(rows)
    print(f"\n  Avg market price of eventual winner: {avg_winner_price:.1%}")

    # HHI (concentration) of market prices per event
    cur.execute("""
        WITH event_prices AS (
            SELECT series_ticker, close_time::date as dt,
                   early_price::float as ep
            FROM kalshi.historical_resolutions
            WHERE series_ticker = ANY(%s)
              AND early_price IS NOT NULL AND result IS NOT NULL
        )
        SELECT series_ticker, dt,
               SUM(ep * ep) / (SUM(ep) * SUM(ep)) as hhi,
               MAX(ep) as max_price,
               COUNT(*) as n_buckets
        FROM event_prices
        GROUP BY series_ticker, dt
        HAVING COUNT(*) >= 3
    """, (series_list,))
    hhi_rows = cur.fetchall()
    if hhi_rows:
        avg_hhi = sum(r[2] for r in hhi_rows) / len(hhi_rows)
        avg_max = sum(r[3] for r in hhi_rows) / len(hhi_rows)
        print(f"  Avg HHI (concentration): {float(avg_hhi):.3f}")
        print(f"  Avg top bucket price: {float(avg_max):.1%}")

    # Strategy test: "buy the field" — buy NO on top 2 buckets
    print(f"\n  Strategy: Buy NO on BOTH rank 1 AND rank 2:")
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
        SELECT rn, ep, result FROM ranked WHERE rn IN (1, 2) AND ep BETWEEN 0.10 AND 0.60
    """, (series_list,))
    strat_rows = cur.fetchall()
    total_pnl = 0
    total_trades = 0
    total_wins = 0
    for rn, ep, result in strat_rows:
        no_price = 1.0 - ep
        if result == 'no':
            total_pnl += (ep / no_price) * 100
            total_wins += 1
        else:
            total_pnl += -100
        total_trades += 1
    if total_trades:
        print(f"    {total_trades} trades, {total_wins} wins ({total_wins*100//total_trades}%), "
              f"P&L: ${total_pnl:+,.0f}")

conn.close()
