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


if __name__ == "__main__":
    cli()
