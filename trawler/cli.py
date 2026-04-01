import logging

import click
from rich.logging import RichHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)


@click.group()
def cli():
    """Kalshi Market Trawler — discover mispriced prediction markets."""
    pass


@cli.group()
def db():
    """Database commands."""
    pass


@db.command()
def init():
    """Initialize the kalshi schema and tables."""
    from trawler.db.schema import init_schema

    init_schema()
    click.echo("Schema initialized.")


@cli.command()
@click.option("--resolutions/--no-resolutions", default=True, help="Also pull historical markets.")
def scan(resolutions):
    """Scan Kalshi for all series and ingest to DB."""
    from trawler.scanner.discover import scan_all_series, ingest_all_resolutions

    scan_all_series()
    if resolutions:
        ingest_all_resolutions()


@cli.command("filter")
def filter_cmd():
    """Apply candidate filters to scanned series."""
    from trawler.scanner.filter import run_filters

    run_filters()


@cli.command()
def calibrate():
    """Compute calibration scores for candidate series."""
    from trawler.calibration.scorer import compute_all

    compute_all()


@cli.command()
@click.argument("ticker")
def analyze(ticker):
    """Run analysis for a specific series ticker."""
    from trawler.analyzers.registry import get_analyzer

    analyzer = get_analyzer(ticker)
    if analyzer is None:
        click.echo(f"No analyzer registered for {ticker}")
        return
    opportunities = analyzer.find_opportunities()
    click.echo(f"Found {len(opportunities)} opportunities for {ticker}")


@cli.command()
def report():
    """Show current opportunities."""
    from trawler.reports.cli_report import show_report

    show_report()


@cli.command()
@click.argument("series_ticker", required=False)
def backtest(series_ticker):
    """Run no-lookahead backtest on resolved markets."""
    from trawler.backtesting.backtest import run_backtest

    run_backtest(series_ticker)


@cli.command("backtest-all")
@click.option("--min-markets", default=20, help="Minimum resolved markets per series")
def backtest_all(min_markets):
    """Run calibration-based backtest on all series."""
    from trawler.backtesting.calibration_backtest import run_calibration_backtest

    run_calibration_backtest(min_markets)


@cli.command("backtest-prophet")
@click.argument("series_ticker", required=False)
def backtest_prophet(series_ticker):
    """Run Prophet-based backtest on time-series markets."""
    from trawler.backtesting.prophet_backtest import run_prophet_backtest

    run_prophet_backtest(series_ticker)


@cli.command("scan-edges")
def scan_edges():
    """Scan candidate series for exploitable edges."""
    from trawler.scanner.edge_scanner import scan_for_edges

    scan_for_edges()


@cli.command("pull-prices")
@click.argument("series_ticker", required=False)
@click.option("--limit", default=None, type=int, help="Max markets to process")
def pull_prices(series_ticker, limit):
    """Pull early trading prices from candlestick data."""
    from trawler.scanner.price_history import pull_early_prices

    pull_early_prices(series_ticker, limit)


@cli.group("fwd")
def fwd():
    """Forward test commands."""
    pass


@fwd.command("scan")
@click.option("--window", default=None, help="Time window label (e.g., 10am, 1pm, 3pm, 5pm)")
def fwd_scan(window):
    """Scan open markets and log today's trading signals."""
    from trawler.forward_test import scan_signals

    scan_signals(window)


@fwd.command("settle")
def fwd_settle():
    """Update settled forward test entries with actual results."""
    from trawler.forward_test import settle_results

    settle_results()


@fwd.command("report")
def fwd_report():
    """Show forward test P&L summary."""
    from trawler.forward_test import report

    report()


if __name__ == "__main__":
    cli()
