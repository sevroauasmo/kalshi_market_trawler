"""
Forward test runner for the second-favorite short strategy.

Runs daily: scans open weather markets, identifies the second-favorite bucket,
computes edge, logs the signal to kalshi.forward_test, and tracks outcome
when the market settles.

Usage:
    poetry run python -m trawler.forward_test scan     # Log today's signals
    poetry run python -m trawler.forward_test settle   # Update settled results
    poetry run python -m trawler.forward_test report   # Show forward test P&L
"""
import logging
import sys
import time as time_mod
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from rich.console import Console
from rich.table import Table

from trawler.api.client import KalshiClient
from trawler.db.connection import get_connection

log = logging.getLogger(__name__)
console = Console()
ET = ZoneInfo("America/New_York")

# Edge curves from backtest (overall YES win rate for second-favorite)
CITY_EDGE = {
    "KXHIGHNY":  {"city": "NYC",     "yes_rate": 0.155, "be_no": 0.845, "max_yes": 0.35},
    "KXHIGHMIA": {"city": "Miami",   "yes_rate": 0.273, "be_no": 0.727, "max_yes": 0.30},
    "KXHIGHCHI": {"city": "Chicago", "yes_rate": 0.281, "be_no": 0.719, "max_yes": 0.28},
    "KXHIGHLAX": {"city": "LA",      "yes_rate": 0.239, "be_no": 0.761, "max_yes": 0.30},
    "KXHIGHAUS": {"city": "Austin",  "yes_rate": 0.225, "be_no": 0.775, "max_yes": 0.30},
    "KXHIGHDEN": {"city": "Denver",  "yes_rate": 0.263, "be_no": 0.737, "max_yes": 0.28},
}


def scan_signals(window_label=None):
    """Scan open weather markets and log signals for today.

    Args:
        window_label: Optional label like '10am', '1pm', '3pm', '5pm'.
                      If None, uses current time.
    """
    console.print(f"[bold]Scanning open weather markets for signals "
                  f"({window_label or 'now'})...[/bold]", highlight=False)

    client = KalshiClient()
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now(ET)
    today = now.date()
    scan_label = window_label or now.strftime("%I%p").lstrip("0").lower()

    signals_logged = 0

    for series, cfg in CITY_EDGE.items():
        city = cfg["city"]
        be_no = cfg["be_no"]
        max_yes = cfg["max_yes"]
        yes_rate = cfg["yes_rate"]

        # Get open markets for this series
        try:
            markets = list(client.get_markets(series_ticker=series, status="open"))
        except Exception as e:
            console.print(f"  [red]{city}: failed to get markets: {e}[/red]")
            continue

        if not markets:
            console.print(f"  {city}: no open markets")
            continue

        # Group by event (close_time date = target day + 1)
        events = {}
        for m in markets:
            # Target date is close_time - 1 day (markets close day after target)
            close_dt = datetime.fromisoformat(m.close_time.replace("Z", "+00:00")) if isinstance(m.close_time, str) else m.close_time
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=timezone.utc)
            target_date = (close_dt - timedelta(days=1)).date()
            events.setdefault(target_date, []).append(m)

        for target_date, event_markets in sorted(events.items()):
            # Only look at markets for tomorrow or today
            if target_date < today or target_date > today + timedelta(days=2):
                continue

            # Rank by last_price (current price, best we have for live markets)
            ranked = sorted(event_markets, key=lambda m: -(m.last_price or 0))
            if len(ranked) < 3:
                continue

            second = ranked[1]
            yes_price = second.last_price or 0
            if yes_price < 0.08 or yes_price > max_yes:
                action = "SKIP"
                ev = 0
            else:
                no_ask = 1 - yes_price  # approximate
                room = be_no - no_ask
                if room < 0.02:
                    action = "SKIP"
                    ev = 0
                else:
                    ev = (1 - yes_rate) * (1 - no_ask) - yes_rate * no_ask
                    action = "SIGNAL" if ev > 0.02 else "MARGINAL"

            # Estimate position size (conservative: $100 base)
            hourly_vol = second.volume or 0
            position = min(100, hourly_vol * 0.3) if hourly_vol > 0 else 100

            # Log to DB
            cur.execute("""
                INSERT INTO kalshi.forward_test
                    (test_date, city, series_ticker, target_date, bucket_sub,
                     bucket_rank, signal_time, yes_price_at_signal, no_ask_at_signal,
                     hourly_volume, position_size, estimated_fill_no, break_even_no,
                     ev_per_dollar, action, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                today, city, series, target_date, second.yes_sub_title,
                2, now, yes_price, 1 - yes_price,
                hourly_vol, position, 1 - yes_price + 0.01, be_no,
                round(ev, 4), action,
                f"window={scan_label} R1={ranked[0].yes_sub_title}@{ranked[0].last_price:.2f}",
            ))
            signals_logged += 1

            status_color = "green" if action == "SIGNAL" else ("yellow" if action == "MARGINAL" else "dim")
            console.print(
                f"  [{status_color}]{city} {target_date}: {second.yes_sub_title} "
                f"YES={yes_price:.0%} NO≈{1-yes_price:.0%} EV={ev:.3f} → {action}[/{status_color}]"
            )

    conn.commit()
    conn.close()
    client.close()
    console.print(f"\nLogged {signals_logged} signals.")


def settle_results():
    """Check settled markets and update forward test entries with results.

    First refreshes market data from Kalshi API for recent markets,
    then matches against unsettled forward test entries.
    """
    console.print("[bold]Settling forward test results...[/bold]")

    # Step 1: Refresh recent market results from API
    client = KalshiClient()
    conn = get_connection()
    cur = conn.cursor()

    # Get unsettled entries
    cur.execute("""
        SELECT id, series_ticker, target_date, bucket_sub, position_size,
               estimated_fill_no, yes_price_at_signal
        FROM kalshi.forward_test
        WHERE actual_result IS NULL AND action IN ('SIGNAL', 'MARGINAL')
        ORDER BY target_date
    """)
    unsettled = cur.fetchall()

    if not unsettled:
        console.print("  No unsettled entries.")
        conn.close()
        client.close()
        return

    # Step 2: For each unsettled entry, check if market has settled
    settled = 0
    for row_id, series, target_date, bucket_sub, position, fill_no, yes_at_signal in unsettled:
        # First check our DB
        cur.execute("""
            SELECT result FROM kalshi.historical_resolutions
            WHERE series_ticker = %s AND yes_sub_title = %s AND result IS NOT NULL
            ORDER BY close_time DESC LIMIT 1
        """, (series, bucket_sub))
        res = cur.fetchone()

        if res is None:
            # Try pulling from API
            try:
                markets = list(client.get_markets(series_ticker=series))
                for m in markets:
                    if m.yes_sub_title == bucket_sub and m.result:
                        res = (m.result,)
                        # Also update our DB
                        from trawler.db.resolutions_repo import upsert_markets
                        upsert_markets([m])
                        break
            except Exception as e:
                log.warning("Failed to check API for %s: %s", series, e)

        if res is None:
            continue

        result = res[0]
        fill = float(fill_no) if fill_no else 0.75
        pos = float(position) if position else 100
        yes_sig = float(yes_at_signal) if yes_at_signal else 0.25

        if result == "no":
            n_contracts = pos / fill
            pnl = n_contracts * (1 - fill)
        else:
            pnl = -pos

        roi = pnl / pos if pos else 0

        cur.execute("""
            UPDATE kalshi.forward_test
            SET actual_result = %s, actual_pnl = %s, settled_at = NOW()
            WHERE id = %s
        """, (result, round(pnl, 2), row_id))
        settled += 1

        color = "green" if pnl > 0 else "red"
        console.print(f"  [{color}]{series} {target_date} {bucket_sub}: "
                       f"result={result} P&L=${pnl:+,.0f} ROI={roi:+.0%}[/{color}]")

    conn.commit()
    conn.close()
    client.close()
    console.print(f"Settled {settled} entries.")


def report():
    """Print forward test summary."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT city, test_date, target_date, bucket_sub, action,
               yes_price_at_signal, ev_per_dollar, position_size,
               actual_result, actual_pnl
        FROM kalshi.forward_test
        ORDER BY test_date, city
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        console.print("No forward test data yet.")
        return

    table = Table(title="Forward Test Log")
    table.add_column("Date")
    table.add_column("City")
    table.add_column("Target")
    table.add_column("Bucket")
    table.add_column("Action")
    table.add_column("YES", justify="right")
    table.add_column("EV/$", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Result")
    table.add_column("P&L", justify="right")

    total_pnl = 0
    total_trades = 0
    total_wins = 0

    for city, test_dt, target_dt, sub, action, yes_p, ev, size, result, pnl in rows:
        if pnl is not None:
            total_pnl += float(pnl)
            total_trades += 1
            if float(pnl) > 0:
                total_wins += 1

        pnl_str = f"${float(pnl):+,.0f}" if pnl is not None else ""
        pnl_style = "green" if pnl and float(pnl) > 0 else ("red" if pnl and float(pnl) < 0 else "")
        result_str = result if result else "pending"

        table.add_row(
            str(test_dt), city, str(target_dt), sub or "", action or "",
            f"{float(yes_p):.0%}" if yes_p else "",
            f"{float(ev):.3f}" if ev else "",
            f"${float(size):.0f}" if size else "",
            result_str,
            f"[{pnl_style}]{pnl_str}[/{pnl_style}]" if pnl_str else "",
        )

    console.print(table)

    if total_trades:
        console.print(f"\nSettled: {total_trades} trades, {total_wins} wins "
                       f"({total_wins*100//total_trades}%), P&L: ${total_pnl:+,.0f}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if cmd == "scan":
        scan_signals()
    elif cmd == "settle":
        settle_results()
    elif cmd == "report":
        report()
    else:
        print(f"Unknown command: {cmd}. Use scan/settle/report.")
