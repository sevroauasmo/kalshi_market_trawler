import logging

from trawler.api.client import KalshiClient
from trawler.db.series_repo import get_all_series_tickers, upsert_series
from trawler.db.resolutions_repo import upsert_markets

log = logging.getLogger(__name__)


def scan_all_series():
    """Pull all series from Kalshi API and upsert to DB."""
    log.info("Scanning all Kalshi series...")
    with KalshiClient() as client:
        series = client.get_all_series()
    log.info("Fetched %d series from API", len(series))
    upsert_series(series)


def ingest_resolutions(series_ticker: str, client: KalshiClient | None = None):
    """Pull all markets for a series and upsert to DB."""
    own_client = client is None
    if own_client:
        client = KalshiClient()
    try:
        markets = list(client.get_markets(series_ticker=series_ticker))
        if markets:
            upsert_markets(markets)
        return len(markets)
    finally:
        if own_client:
            client.close()


def ingest_all_resolutions():
    """Pull markets for every series in the DB."""
    tickers = get_all_series_tickers()
    log.info("Ingesting markets for %d series...", len(tickers))
    total = 0
    with KalshiClient() as client:
        for i, ticker in enumerate(tickers):
            try:
                count = ingest_resolutions(ticker, client)
                total += count
                if (i + 1) % 100 == 0:
                    log.info("  Progress: %d/%d series, %d markets total", i + 1, len(tickers), total)
            except Exception as e:
                log.warning("Failed to ingest %s: %s", ticker, e)
    log.info("Done. Ingested %d total markets across %d series", total, len(tickers))
