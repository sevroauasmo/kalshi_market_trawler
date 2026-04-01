"""Rich-formatted terminal report for detected opportunities."""
import logging

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from trawler.db.connection import get_connection

log = logging.getLogger(__name__)
console = Console()


def _fetch_opportunities() -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.market_ticker, o.series_ticker, o.market_title,
                       o.market_price, o.our_estimate, o.edge, o.confidence,
                       o.reasoning, o.recommended_side, o.recommended_position,
                       o.detected_at, o.status,
                       c.avg_calibration_error, c.total_markets_resolved
                FROM kalshi.opportunities o
                LEFT JOIN kalshi.calibration_scores c
                    ON c.series_ticker = o.series_ticker
                WHERE o.status = 'open'
                ORDER BY ABS(o.edge) * CASE o.confidence
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    ELSE 1
                END DESC
            """)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_pipeline_stats() -> dict:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM kalshi.series_catalog")
            total_series = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM kalshi.series_catalog WHERE candidate_status = 'candidate'")
            candidates = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM kalshi.historical_resolutions")
            total_markets = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM kalshi.historical_resolutions WHERE result IS NOT NULL")
            resolved = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM kalshi.calibration_scores")
            calibrated = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM kalshi.opportunities WHERE status = 'open'")
            open_opps = cur.fetchone()[0]
            return {
                "total_series": total_series,
                "candidates": candidates,
                "total_markets": total_markets,
                "resolved_markets": resolved,
                "calibrated_series": calibrated,
                "open_opportunities": open_opps,
            }
    finally:
        conn.close()


def _confidence_color(conf: str) -> str:
    return {"high": "green", "medium": "yellow", "low": "dim"}.get(conf, "white")


def _edge_display(edge: float) -> Text:
    pp = edge * 100
    color = "green" if edge > 0 else "red"
    return Text(f"{pp:+.1f}pp", style=color)


def _side_display(side: str) -> Text:
    color = "green bold" if side == "yes" else "red bold"
    return Text(side.upper(), style=color)


def show_report():
    """Print the full opportunity report to the terminal."""
    stats = _fetch_pipeline_stats()
    opps = _fetch_opportunities()

    # Pipeline summary
    summary = (
        f"Series: {stats['total_series']:,} scanned, {stats['candidates']:,} candidates\n"
        f"Markets: {stats['total_markets']:,} ingested, {stats['resolved_markets']:,} resolved\n"
        f"Calibrated: {stats['calibrated_series']:,} series\n"
        f"Open opportunities: {stats['open_opportunities']:,}"
    )
    console.print(Panel(summary, title="Pipeline Summary", border_style="blue"))

    if not opps:
        console.print("\n[dim]No open opportunities found. Run [bold]trawler analyze <TICKER>[/bold] first.[/dim]")
        return

    # Opportunities table
    table = Table(title="Opportunities (sorted by edge x confidence)", show_lines=True)
    table.add_column("Ticker", style="cyan", no_wrap=True)
    table.add_column("Series", style="dim")
    table.add_column("Market Price", justify="right")
    table.add_column("Our Estimate", justify="right")
    table.add_column("Edge", justify="right")
    table.add_column("Conf.", justify="center")
    table.add_column("Side", justify="center")
    table.add_column("Size", justify="right")
    table.add_column("Cal. Err", justify="right", style="dim")

    for o in opps:
        mp = float(o["market_price"] or 0)
        est = float(o["our_estimate"] or 0)
        edge = float(o["edge"] or 0)
        conf = o["confidence"] or ""
        cal_err = o.get("avg_calibration_error")
        cal_str = f"{float(cal_err)*100:.1f}%" if cal_err is not None else "-"

        table.add_row(
            o["market_ticker"],
            o["series_ticker"],
            f"{mp*100:.0f}c",
            f"{est*100:.0f}c",
            _edge_display(edge),
            Text(conf, style=_confidence_color(conf)),
            _side_display(o["recommended_side"] or ""),
            f"${float(o['recommended_position'] or 0):.0f}",
            cal_str,
        )

    console.print(table)

    # Detail cards for high-confidence opportunities
    high_conf = [o for o in opps if o["confidence"] == "high"]
    if high_conf:
        console.print(f"\n[bold green]High-confidence opportunities ({len(high_conf)}):[/bold green]")
        for o in high_conf:
            edge = float(o["edge"] or 0)
            reasoning = o.get("reasoning", "")
            title = o.get("market_title", o["market_ticker"])
            console.print(Panel(
                f"[bold]{title}[/bold]\n\n"
                f"Edge: {edge*100:+.1f}pp | Side: {(o['recommended_side'] or '').upper()} | "
                f"Position: ${float(o['recommended_position'] or 0):.0f}\n\n"
                f"{reasoning}",
                title=o["market_ticker"],
                border_style="green",
            ))
