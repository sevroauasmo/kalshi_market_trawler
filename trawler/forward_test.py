"""
Forward test / live trading runner for the second-favorite short strategy.

Strategy: Buy NO on the second-most-popular temperature bucket.
The market systematically overprices the runner-up — it wins only 15.5% in NYC
but gets priced at 25-35%. Buying NO at 65-83c has positive EV.

Modes:
    PAPER (default) — log signals and track hypothetical P&L
    LIVE           — actually place orders via Kalshi API

Commands:
    trawler fwd scan [--window 3pm]  — scan markets, log signals
    trawler fwd settle               — update settled results
    trawler fwd report               — show P&L summary
"""
import logging
import os
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

# ─── CONFIGURATION ───────────────────────────────────────────────
LIVE_MODE = os.environ.get("KALSHI_LIVE_MODE", "false").lower() == "true"

# The window at which we TRADE (place real orders in live mode).
# Other windows are logged for monitoring only.
# Backtest showed 3pm ET as best entry. Second-fav changes 60% of the time
# between windows, so we must lock to ONE window.
TRADE_WINDOW = "3pm"

# Cities to trade. Based on realistic sim:
# NYC: +$8,517/98 days, 85% win rate, 14.2% ROI — PRIMARY
# Miami: +$216/98 days, 88% win rate — small sidecar
ACTIVE_CITIES = {
    "KXHIGHNY": {
        "city": "NYC",
        "yes_rate": 0.155,      # second-fav wins 15.5% of the time
        "be_no": 0.845,         # break-even NO price
        "max_yes": 0.35,        # don't short if second-fav > 35c
        "min_yes": 0.08,        # don't short if second-fav < 8c
        "max_position": 600,    # max $ to deploy per day (based on avg from sim)
        "fill_pct": 0.30,       # buy up to 30% of hourly volume
        "enabled": True,
    },
    "KXHIGHMIA": {
        "city": "Miami",
        "yes_rate": 0.273,
        "be_no": 0.727,
        "max_yes": 0.30,
        "min_yes": 0.08,
        "max_position": 100,
        "fill_pct": 0.30,
        "enabled": True,
    },
}


def scan_signals(window_label=None):
    """Scan open weather markets and log signals."""
    mode_str = "[red bold]LIVE[/red bold]" if LIVE_MODE else "[cyan]PAPER[/cyan]"
    console.print(f"Scanning markets ({window_label or 'now'}) — {mode_str} mode",
                  highlight=False)

    client = KalshiClient()
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now(ET)
    today = now.date()
    scan_label = window_label or now.strftime("%I%p").lstrip("0").lower()

    signals = []

    for series, cfg in ACTIVE_CITIES.items():
        if not cfg["enabled"]:
            continue

        city = cfg["city"]
        be_no = cfg["be_no"]
        max_yes = cfg["max_yes"]
        min_yes = cfg["min_yes"]
        yes_rate = cfg["yes_rate"]
        max_pos = cfg["max_position"]

        try:
            markets = list(client.get_markets(series_ticker=series, status="open"))
        except Exception as e:
            console.print(f"  [red]{city}: failed: {e}[/red]")
            continue

        if not markets:
            console.print(f"  [dim]{city}: no open markets[/dim]")
            continue

        # Group by target date
        events = {}
        for m in markets:
            close_str = m.close_time if isinstance(m.close_time, str) else m.close_time.isoformat()
            close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            target_date = (close_dt - timedelta(days=1)).date()
            events.setdefault(target_date, []).append(m)

        for target_date, event_markets in sorted(events.items()):
            if target_date < today or target_date > today + timedelta(days=2):
                continue

            ranked = sorted(event_markets, key=lambda m: -(m.last_price or 0))
            if len(ranked) < 3:
                continue

            fav = ranked[0]
            second = ranked[1]
            yes_price = second.last_price or 0

            # Determine action
            if yes_price < min_yes or yes_price > max_yes:
                action = "SKIP"
                ev = 0
                reason = f"YES={yes_price:.0%} outside {min_yes:.0%}-{max_yes:.0%}"
            else:
                no_ask = 1 - yes_price
                room = be_no - no_ask
                if room < 0.02:
                    action = "SKIP"
                    ev = 0
                    reason = f"NO={no_ask:.0%} too close to BE={be_no:.0%}"
                else:
                    ev = (1 - yes_rate) * (1 - no_ask) - yes_rate * no_ask
                    if ev > 0.02:
                        action = "SIGNAL"
                        reason = f"EV=${ev:.3f}/dollar"
                    elif ev > 0:
                        action = "MARGINAL"
                        reason = f"EV=${ev:.3f}/dollar (thin)"
                    else:
                        action = "SKIP"
                        reason = f"EV=${ev:.3f} negative"

            # Position sizing
            position = min(max_pos, 100) if action == "SIGNAL" else 0

            # Log to DB — upsert per (date, city, target, window)
            cur.execute("""
                INSERT INTO kalshi.forward_test
                    (test_date, city, series_ticker, target_date, bucket_sub,
                     bucket_rank, signal_time, yes_price_at_signal, no_ask_at_signal,
                     hourly_volume, position_size, estimated_fill_no, break_even_no,
                     ev_per_dollar, action, scan_window, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (test_date, city, target_date, scan_window)
                DO UPDATE SET
                    bucket_sub = EXCLUDED.bucket_sub,
                    yes_price_at_signal = EXCLUDED.yes_price_at_signal,
                    no_ask_at_signal = EXCLUDED.no_ask_at_signal,
                    ev_per_dollar = EXCLUDED.ev_per_dollar,
                    action = EXCLUDED.action,
                    signal_time = EXCLUDED.signal_time,
                    notes = EXCLUDED.notes
            """, (
                today, city, series, target_date, second.yes_sub_title,
                2, now, yes_price, 1 - yes_price,
                second.volume or 0, position, 1 - yes_price + 0.01, be_no,
                round(ev, 4), action, scan_label,
                f"fav={fav.yes_sub_title}@{fav.last_price:.2f} {reason}",
            ))

            color = {"SIGNAL": "green", "MARGINAL": "yellow", "SKIP": "dim"}[action]
            console.print(
                f"  [{color}]{city} {target_date}: {second.yes_sub_title} "
                f"YES={yes_price:.0%} EV={ev:.3f} → {action} "
                f"(fav={fav.yes_sub_title}@{fav.last_price:.0%})[/{color}]"
            )

            if action == "SIGNAL":
                signals.append({
                    "city": city, "series": series, "target": target_date,
                    "ticker": second.ticker, "sub": second.yes_sub_title,
                    "yes_price": yes_price, "position": position,
                })

    conn.commit()
    conn.close()

    # ─── LIVE MODE: Place orders ─────────────────────────────────
    # Only trade at the designated window (backtest validated 3pm ET)
    is_trade_window = scan_label == TRADE_WINDOW
    if LIVE_MODE and signals and not is_trade_window:
        console.print(f"\n[yellow]LIVE MODE: {len(signals)} signals but window={scan_label}, "
                      f"not {TRADE_WINDOW}. Logging only, not trading.[/yellow]")
    elif LIVE_MODE and signals and is_trade_window:
        console.print(f"\n[red bold]LIVE MODE ({TRADE_WINDOW} window): "
                      f"Placing {len(signals)} orders[/red bold]")

        trade_client = KalshiClient(authenticated=True)

        # Safety: check balance
        try:
            bal = trade_client.get_balance()
            balance_dollars = bal.get("balance", 0) / 100
            console.print(f"  Balance: ${balance_dollars:.2f}")
        except Exception as e:
            console.print(f"  [red]Balance check failed: {e}. Aborting.[/red]")
            trade_client.close()
            client.close()
            return

        # Safety: check what we've already traded today to avoid double-ordering
        conn2 = get_connection()
        cur2 = conn2.cursor()
        cur2.execute("""
            SELECT target_date, city FROM kalshi.forward_test
            WHERE test_date = %s AND notes LIKE '%%LIVE order=%%'
        """, (today,))
        already_traded = set((r[0], r[1]) for r in cur2.fetchall())
        conn2.close()

        for sig in signals:
            ticker = sig["ticker"]
            city = sig["city"]
            target = sig["target"]

            # Don't double-trade the same city+target
            if (target, city) in already_traded:
                console.print(f"  [dim]{city} {target}: already traded today, skipping[/dim]")
                continue

            yes_price = sig["yes_price"]
            no_price_cents = int((1 - yes_price) * 100)

            # Guard: no_price must be between 1-99
            if no_price_cents <= 0 or no_price_cents >= 100:
                console.print(f"  [red]{city} {ticker}: invalid NO price {no_price_cents}c, skipping[/red]")
                continue

            position_dollars = sig["position"]
            contracts = int(position_dollars * 100 / no_price_cents)
            total_cost = contracts * no_price_cents / 100

            # Safety: don't exceed balance (keep $1 buffer)
            if total_cost > balance_dollars - 1.00:
                contracts = int((balance_dollars - 1.00) * 100 / no_price_cents)
                total_cost = contracts * no_price_cents / 100

            if contracts <= 0:
                console.print(f"  [yellow]{city}: insufficient balance "
                              f"(need ~${position_dollars:.0f}, have ${balance_dollars:.2f})[/yellow]")
                continue

            console.print(f"  → BUY {contracts} NO @ {no_price_cents}c on {ticker} (${total_cost:.2f})")

            try:
                order = trade_client.place_order(
                    ticker=ticker,
                    side="no",
                    count=contracts,
                    price_cents=no_price_cents,
                    action="buy",
                    order_type="limit",
                )
                od = order.get("order", {})
                status = od.get("status", "unknown")
                filled = od.get("fill_count_fp", "0")
                order_id = od.get("order_id", "")
                fees = od.get("taker_fees_dollars", "0")

                color = "green" if status == "executed" else "yellow"
                console.print(f"  [{color}]{status.upper()}: filled={filled}, "
                              f"fees=${float(fees):.2f}, id={order_id[:12]}[/{color}]")

                # Log order to DB
                conn3 = get_connection()
                cur3 = conn3.cursor()
                cur3.execute("""
                    UPDATE kalshi.forward_test
                    SET notes = COALESCE(notes, '') || %s,
                        position_size = %s
                    WHERE test_date = %s AND city = %s
                      AND target_date = %s AND scan_window = %s
                """, (
                    f" | LIVE order={order_id} {status} "
                    f"filled={filled} cost=${total_cost:.2f} fees=${float(fees):.2f}",
                    total_cost, today, city, target, scan_label,
                ))
                conn3.commit()
                conn3.close()

                balance_dollars -= total_cost
                already_traded.add((target, city))

            except Exception as e:
                console.print(f"  [red]FAILED: {e}[/red]")
                log.error("Order failed for %s: %s", ticker, e, exc_info=True)

        trade_client.close()

    elif LIVE_MODE and not signals:
        console.print("\n[yellow]LIVE MODE: no signals[/yellow]")

    client.close()
    console.print(f"\nLogged {len(signals)} signals, "
                  f"{sum(1 for s in ACTIVE_CITIES.values() if s['enabled'])} cities active.")


def settle_results():
    """Check settled markets and update forward test entries."""
    console.print("[bold]Settling forward test results...[/bold]")

    client = KalshiClient()
    conn = get_connection()
    cur = conn.cursor()

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

    # Also refresh our market data for these series
    series_to_refresh = set(r[1] for r in unsettled)
    for series in series_to_refresh:
        try:
            from trawler.scanner.discover import ingest_resolutions
            ingest_resolutions(series, client)
        except Exception as e:
            log.warning("Failed to refresh %s: %s", series, e)

    settled = 0
    now = datetime.now(timezone.utc)

    for row_id, series, target_date, bucket_sub, position, fill_no, yes_at_signal in unsettled:
        # Safety: don't settle markets that haven't closed yet
        # Markets close at ~05:00 UTC the day AFTER target_date
        market_close = datetime(target_date.year, target_date.month, target_date.day,
                                 6, 0, tzinfo=timezone.utc) + timedelta(days=1)
        if now < market_close:
            continue  # market hasn't closed yet

        # Match by close_time date (= target_date + 1 day) to get the RIGHT day's market
        close_date = target_date + timedelta(days=1)
        cur.execute("""
            SELECT result FROM kalshi.historical_resolutions
            WHERE series_ticker = %s AND yes_sub_title = %s
              AND close_time::date = %s AND result IS NOT NULL
            LIMIT 1
        """, (series, bucket_sub, close_date))
        res = cur.fetchone()

        if res is None:
            # Try API — but only look for markets closing on the right date
            try:
                for m in client.get_markets(series_ticker=series):
                    if not m.yes_sub_title == bucket_sub or not m.result:
                        continue
                    # Check close_time matches
                    close_str = m.close_time if isinstance(m.close_time, str) else m.close_time.isoformat()
                    m_close = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    if m_close.date() == close_date:
                        res = (m.result,)
                        break
            except Exception:
                pass

        if res is None:
            continue

        result = res[0]
        fill = float(fill_no) if fill_no else 0.75
        pos = float(position) if position else 100

        if result == "no":
            n_contracts = pos / fill
            pnl = n_contracts * (1 - fill)
        else:
            pnl = -pos

        roi = pnl / pos * 100 if pos else 0

        cur.execute("""
            UPDATE kalshi.forward_test
            SET actual_result = %s, actual_pnl = %s, settled_at = NOW()
            WHERE id = %s
        """, (result, round(pnl, 2), row_id))
        settled += 1

        color = "green" if pnl > 0 else "red"
        console.print(f"  [{color}]{series} {target_date} {bucket_sub}: "
                       f"{result} P&L=${pnl:+,.0f} ROI={roi:+.0f}%[/{color}]")

    conn.commit()
    conn.close()
    client.close()
    console.print(f"Settled {settled} entries.")


def report():
    """Print forward test summary with running P&L."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT city, test_date, target_date, bucket_sub, action,
               yes_price_at_signal::float, no_ask_at_signal::float,
               ev_per_dollar::float, position_size::float,
               actual_result, actual_pnl::float, notes
        FROM kalshi.forward_test
        ORDER BY test_date, city, target_date
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        console.print("No forward test data yet. Run: trawler fwd scan")
        return

    # Signals table
    table = Table(title="Forward Test Log")
    table.add_column("Date", style="dim")
    table.add_column("City")
    table.add_column("Target")
    table.add_column("Bucket")
    table.add_column("Act", justify="center")
    table.add_column("YES", justify="right")
    table.add_column("NO", justify="right")
    table.add_column("EV/$", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Result", justify="center")
    table.add_column("P&L", justify="right")

    total_pnl = 0
    total_deployed = 0
    total_trades = 0
    total_wins = 0
    cum_pnl = 0
    peak = 0
    max_dd = 0

    for (city, test_dt, target_dt, sub, action, yes_p, no_p,
         ev, size, result, pnl, notes) in rows:

        if action not in ("SIGNAL", "MARGINAL"):
            continue

        if pnl is not None:
            cum_pnl += pnl
            total_pnl += pnl
            total_trades += 1
            if pnl > 0:
                total_wins += 1
            total_deployed += (size or 0)
            peak = max(peak, cum_pnl)
            max_dd = max(max_dd, peak - cum_pnl)

        pnl_str = f"${pnl:+,.0f}" if pnl is not None else ""
        pnl_style = "green" if pnl and pnl > 0 else ("red" if pnl and pnl < 0 else "")
        result_str = result.upper() if result else "[yellow]pending[/yellow]"

        table.add_row(
            str(test_dt), city, str(target_dt), sub or "",
            f"[green]{action}[/green]" if action == "SIGNAL" else f"[yellow]{action}[/yellow]",
            f"{yes_p:.0%}" if yes_p else "",
            f"{no_p:.0%}" if no_p else "",
            f"{ev:.3f}" if ev else "",
            f"${size:.0f}" if size else "",
            result_str,
            f"[{pnl_style}]{pnl_str}[/{pnl_style}]" if pnl_str else "",
        )

    console.print(table)

    # Summary stats
    if total_trades:
        roi = total_pnl / total_deployed * 100 if total_deployed else 0
        console.print(f"\n[bold]Summary:[/bold]")
        console.print(f"  Trades: {total_trades}, Wins: {total_wins} "
                       f"({total_wins*100//total_trades}%)")
        console.print(f"  P&L: ${total_pnl:+,.0f}, Deployed: ${total_deployed:,.0f}, "
                       f"ROI: {roi:+.1f}%")
        console.print(f"  Max drawdown: ${max_dd:,.0f}")

    pending = sum(1 for r in rows if r[4] in ("SIGNAL", "MARGINAL") and r[9] is None)
    if pending:
        console.print(f"\n  [yellow]{pending} signals pending settlement[/yellow]")

    console.print(f"\n  Mode: {'[red bold]LIVE[/red bold]' if LIVE_MODE else '[cyan]PAPER[/cyan]'}")
    console.print(f"  To go live: export KALSHI_LIVE_MODE=true")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if cmd == "scan":
        window = sys.argv[2] if len(sys.argv) > 2 else None
        scan_signals(window)
    elif cmd == "settle":
        settle_results()
    elif cmd == "report":
        report()
    else:
        print(f"Unknown command: {cmd}. Use scan/settle/report.")
