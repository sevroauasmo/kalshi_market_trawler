"""
Calibration-based backtester — works on ANY series without custom data sources.

For each resolved market, predicts using ONLY prior resolved markets from the
SAME series (no lookahead). Builds a calibration curve from historical price
bins to compute expected hit rates.

Model: "markets in this series priced at X historically resolve yes Y% of the
time — if Y != X, there's an edge."
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.extras import execute_batch
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from trawler.db.connection import get_connection

log = logging.getLogger(__name__)
console = Console()

MODEL_VERSION = "v1_calibration"
NUM_BINS = 10
MIN_HISTORY = 30     # minimum prior resolved markets before predicting
MIN_BIN_COUNT = 3    # minimum markets in a bin to trust its hit rate


def _price_to_bin(price: float) -> int:
    """Map a price in [0, 1] to a bin index 0-9."""
    b = int(price * NUM_BINS)
    return max(0, min(NUM_BINS - 1, b))


def _build_calibration_curve(prior_markets: list[dict]) -> dict:
    """Build calibration curve from prior markets.

    Returns dict with:
        'bins': list of (count, hit_rate) per bin
        'overall_rate': overall yes rate across all markets
    """
    bin_counts = [0] * NUM_BINS
    bin_yes = [0] * NUM_BINS
    total_yes = 0

    for m in prior_markets:
        price = float(m["last_price"])
        b = _price_to_bin(price)
        bin_counts[b] += 1
        if m["result"] == "yes":
            bin_yes[b] += 1
            total_yes += 1

    overall_rate = total_yes / len(prior_markets) if prior_markets else 0.5

    bins = []
    for i in range(NUM_BINS):
        if bin_counts[i] >= MIN_BIN_COUNT:
            bins.append((bin_counts[i], bin_yes[i] / bin_counts[i]))
        else:
            bins.append((bin_counts[i], None))  # insufficient data

    return {"bins": bins, "overall_rate": overall_rate}


def calibration_predict(series_ticker: str) -> list[dict]:
    """Run calibration-based prediction for all resolved markets in a series.

    Walks through markets chronologically. For each market after the first
    MIN_HISTORY, builds a calibration curve from prior markets only and
    estimates probability based on the curve.

    Returns list of result dicts.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT market_ticker, series_ticker, title, yes_sub_title,
                  result, last_price, close_time
           FROM kalshi.historical_resolutions
           WHERE series_ticker = %s
             AND result IS NOT NULL
             AND last_price IS NOT NULL
           ORDER BY close_time""",
        (series_ticker,),
    )
    cols = [d[0] for d in cur.description]
    markets = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    if not markets:
        return []

    results = []

    for i, market in enumerate(markets):
        if i < MIN_HISTORY:
            continue

        prior = markets[:i]
        curve = _build_calibration_curve(prior)

        market_price = float(market["last_price"])
        if market_price <= 0 or market_price >= 1:
            market_price = max(0.01, min(0.99, market_price))

        b = _price_to_bin(market_price)
        bin_count, bin_rate = curve["bins"][b]

        if bin_rate is not None:
            our_estimate = bin_rate
        else:
            # Fall back to overall rate
            our_estimate = curve["overall_rate"]

        # Clamp to avoid extreme values
        our_estimate = max(0.001, min(0.999, our_estimate))

        outcome = 1 if market["result"] == "yes" else 0
        edge = our_estimate - market_price

        our_brier = (our_estimate - outcome) ** 2
        mkt_brier = (market_price - outcome) ** 2

        # Simulate bet
        pnl = None
        if abs(edge) > 0.10:
            if edge > 0:
                # Buy YES
                pnl = 100.0 * (1 - market_price) / market_price if outcome == 1 else -100.0
            else:
                # Buy NO
                pnl = 100.0 * market_price / (1 - market_price) if outcome == 0 else -100.0

        results.append({
            "market_ticker": market["market_ticker"],
            "series_ticker": series_ticker,
            "title": market["title"],
            "yes_sub_title": market["yes_sub_title"],
            "close_time": market["close_time"],
            "result": market["result"],
            "outcome": outcome,
            "market_price": market_price,
            "our_estimate": our_estimate,
            "edge": edge,
            "our_brier": our_brier,
            "mkt_brier": mkt_brier,
            "pnl": pnl,
            "bet_taken": pnl is not None,
        })

    return results


def _ensure_model_version_in_pk():
    """Alter PK to include model_version if it's currently just market_ticker."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Check current PK columns
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = 'kalshi.backtest_results'::regclass
              AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """)
        pk_cols = [r[0] for r in cur.fetchall()]

        if pk_cols == ["market_ticker"]:
            log.info("Altering backtest_results PK to include model_version")
            # Set default for existing rows
            cur.execute("""
                UPDATE kalshi.backtest_results
                SET model_version = 'v1_naive'
                WHERE model_version IS NULL
            """)
            cur.execute("""
                ALTER TABLE kalshi.backtest_results
                DROP CONSTRAINT backtest_results_pkey
            """)
            cur.execute("""
                ALTER TABLE kalshi.backtest_results
                ADD PRIMARY KEY (market_ticker, model_version)
            """)
            conn.commit()
            console.print("[green]Updated PK to (market_ticker, model_version)[/green]")
        else:
            conn.rollback()
    except Exception as e:
        conn.rollback()
        log.warning(f"PK migration note: {e}")
    finally:
        conn.close()


def _save_calibration_results(all_results: list[dict]):
    """Persist calibration backtest results to kalshi.backtest_results."""
    if not all_results:
        return

    _ensure_model_version_in_pk()

    rows = []
    for r in all_results:
        rows.append((
            r["market_ticker"],
            r["series_ticker"],
            r["title"],
            r["yes_sub_title"],
            r["close_time"],
            r["result"],
            r["market_price"],
            r["our_estimate"],
            r["edge"],
            r["our_brier"],
            r["mkt_brier"],
            r["pnl"],
            r["bet_taken"],
            MODEL_VERSION,
        ))

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO kalshi.backtest_results
                    (market_ticker, series_ticker, market_title, yes_sub_title,
                     close_time, result, market_price, our_estimate, edge,
                     our_brier, mkt_brier, pnl, bet_taken, model_version,
                     backtested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (market_ticker, model_version) DO UPDATE SET
                    our_estimate = EXCLUDED.our_estimate,
                    edge = EXCLUDED.edge,
                    our_brier = EXCLUDED.our_brier,
                    mkt_brier = EXCLUDED.mkt_brier,
                    pnl = EXCLUDED.pnl,
                    bet_taken = EXCLUDED.bet_taken,
                    backtested_at = NOW()
            """, rows, page_size=500)
        conn.commit()
        log.info(f"Saved {len(rows)} calibration backtest results to DB")
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to save calibration results: {e}")
        console.print(f"[red]Failed to save results to DB: {e}[/red]")
        raise
    finally:
        conn.close()


def run_calibration_backtest(min_markets: int = 20):
    """Run calibration-based backtest on all series with enough resolved markets."""
    console.print(f"\n[bold]Calibration Backtest[/bold]")
    console.print(f"Finding series with >= {min_markets} resolved markets...\n")

    # Find eligible series
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT series_ticker, COUNT(*) as cnt
        FROM kalshi.historical_resolutions
        WHERE result IS NOT NULL
          AND last_price IS NOT NULL
        GROUP BY series_ticker
        HAVING COUNT(*) >= %s
        ORDER BY COUNT(*) DESC
    """, (min_markets,))
    series_list = cur.fetchall()
    conn.close()

    console.print(f"Found [bold]{len(series_list)}[/bold] series with >= {min_markets} resolved markets")
    total_resolved = sum(r[1] for r in series_list)
    console.print(f"Total resolved markets: [bold]{total_resolved:,}[/bold]\n")

    # Run calibration on each series
    all_results = []
    series_summaries = []

    with Progress() as progress:
        task = progress.add_task("Backtesting series...", total=len(series_list))
        for series_ticker, market_count in series_list:
            results = calibration_predict(series_ticker)
            all_results.extend(results)

            if results:
                bets = [r for r in results if r["pnl"] is not None]
                total_pnl = sum(r["pnl"] for r in bets)
                n_bets = len(bets)
                wins = sum(1 for r in bets if r["pnl"] > 0)
                avg_our_brier = sum(r["our_brier"] for r in results) / len(results)
                avg_mkt_brier = sum(r["mkt_brier"] for r in results) / len(results)

                series_summaries.append({
                    "series": series_ticker,
                    "total_markets": market_count,
                    "scored": len(results),
                    "n_bets": n_bets,
                    "wins": wins,
                    "total_pnl": total_pnl,
                    "avg_our_brier": avg_our_brier,
                    "avg_mkt_brier": avg_mkt_brier,
                    "brier_edge": avg_mkt_brier - avg_our_brier,
                })

            progress.advance(task)

    # Save to DB
    console.print(f"\nSaving {len(all_results):,} results to DB...")
    _save_calibration_results(all_results)
    console.print(f"[green]Saved {len(all_results):,} results to kalshi.backtest_results (model_version='{MODEL_VERSION}')[/green]\n")

    # Sort by total P&L for display
    series_summaries.sort(key=lambda x: x["total_pnl"], reverse=True)

    # Full summary table
    summary_table = Table(
        title=f"Calibration Backtest — All {len(series_summaries)} Series (sorted by P&L)",
        show_lines=False,
    )
    summary_table.add_column("Series", style="cyan", max_width=25)
    summary_table.add_column("Mkts", justify="right")
    summary_table.add_column("Scored", justify="right")
    summary_table.add_column("Our Brier", justify="right")
    summary_table.add_column("Mkt Brier", justify="right")
    summary_table.add_column("B.Edge", justify="right")
    summary_table.add_column("Bets", justify="right")
    summary_table.add_column("Wins", justify="right")
    summary_table.add_column("Win%", justify="right")
    summary_table.add_column("P&L", justify="right")

    grand_pnl = 0.0
    grand_bets = 0
    grand_wins = 0
    grand_scored = 0

    for s in series_summaries:
        pnl_style = "green" if s["total_pnl"] >= 0 else "red"
        brier_style = "green" if s["brier_edge"] > 0 else "red"
        win_rate = f"{s['wins'] / s['n_bets'] * 100:.0f}%" if s["n_bets"] > 0 else "-"

        summary_table.add_row(
            s["series"],
            str(s["total_markets"]),
            str(s["scored"]),
            f"{s['avg_our_brier']:.4f}",
            f"{s['avg_mkt_brier']:.4f}",
            f"[{brier_style}]{s['brier_edge']:+.4f}[/{brier_style}]",
            str(s["n_bets"]),
            str(s["wins"]),
            win_rate,
            f"[{pnl_style}]${s['total_pnl']:+,.0f}[/{pnl_style}]",
        )

        grand_pnl += s["total_pnl"]
        grand_bets += s["n_bets"]
        grand_wins += s["wins"]
        grand_scored += s["scored"]

    console.print(summary_table)

    # Overall stats
    console.print(f"\n[bold]Overall Summary[/bold]")
    console.print(f"  Series backtested: {len(series_summaries)}")
    console.print(f"  Markets scored:    {grand_scored:,}")
    console.print(f"  Total bets:        {grand_bets:,}")
    if grand_bets > 0:
        console.print(f"  Win rate:          {grand_wins / grand_bets * 100:.1f}%")
        pnl_color = "green" if grand_pnl >= 0 else "red"
        console.print(f"  Total P&L:         [{pnl_color}]${grand_pnl:+,.0f}[/{pnl_color}]")

    # Top 10 and Bottom 10
    if len(series_summaries) >= 10:
        console.print(f"\n[bold]Top 10 Series by P&L[/bold]")
        top_table = Table(show_lines=False)
        top_table.add_column("Rank", justify="right")
        top_table.add_column("Series", style="cyan")
        top_table.add_column("Bets", justify="right")
        top_table.add_column("Wins", justify="right")
        top_table.add_column("Win%", justify="right")
        top_table.add_column("P&L", justify="right")
        top_table.add_column("B.Edge", justify="right")

        for i, s in enumerate(series_summaries[:10]):
            win_rate = f"{s['wins'] / s['n_bets'] * 100:.0f}%" if s["n_bets"] > 0 else "-"
            brier_style = "green" if s["brier_edge"] > 0 else "red"
            top_table.add_row(
                str(i + 1),
                s["series"],
                str(s["n_bets"]),
                str(s["wins"]),
                win_rate,
                f"[green]${s['total_pnl']:+,.0f}[/green]",
                f"[{brier_style}]{s['brier_edge']:+.4f}[/{brier_style}]",
            )
        console.print(top_table)

        console.print(f"\n[bold]Bottom 10 Series by P&L[/bold]")
        bottom_table = Table(show_lines=False)
        bottom_table.add_column("Rank", justify="right")
        bottom_table.add_column("Series", style="cyan")
        bottom_table.add_column("Bets", justify="right")
        bottom_table.add_column("Wins", justify="right")
        bottom_table.add_column("Win%", justify="right")
        bottom_table.add_column("P&L", justify="right")
        bottom_table.add_column("B.Edge", justify="right")

        for i, s in enumerate(series_summaries[-10:]):
            win_rate = f"{s['wins'] / s['n_bets'] * 100:.0f}%" if s["n_bets"] > 0 else "-"
            brier_style = "green" if s["brier_edge"] > 0 else "red"
            bottom_table.add_row(
                str(len(series_summaries) - 9 + i),
                s["series"],
                str(s["n_bets"]),
                str(s["wins"]),
                win_rate,
                f"[red]${s['total_pnl']:+,.0f}[/red]",
                f"[{brier_style}]{s['brier_edge']:+.4f}[/{brier_style}]",
            )
        console.print(bottom_table)

    # Series with positive edge
    positive_edge = [s for s in series_summaries if s["total_pnl"] > 0 and s["n_bets"] > 0]
    console.print(f"\n[bold]Series with positive P&L: {len(positive_edge)} / {len(series_summaries)}[/bold]")
    console.print()
