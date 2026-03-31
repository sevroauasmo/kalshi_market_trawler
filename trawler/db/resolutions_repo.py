import logging

import psycopg2.extras

from trawler.api.models import Market
from trawler.db.connection import get_connection

log = logging.getLogger(__name__)


def upsert_markets(markets: list[Market]):
    """Upsert a batch of markets into kalshi.historical_resolutions."""
    if not markets:
        return
    # Filter out markets with no series_ticker (FK constraint)
    markets = [m for m in markets if m.series_ticker]
    if not markets:
        return
    conn = get_connection()
    try:
        rows = [
            (
                m.ticker,
                m.series_ticker,
                m.event_ticker,
                m.title,
                m.yes_sub_title,
                m.no_sub_title,
                m.result,
                m.last_price,
                m.volume,
                m.open_interest,
                m.open_time or None,
                m.close_time or None,
                m.expiration_time or None,
            )
            for m in markets
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO kalshi.historical_resolutions
                    (market_ticker, series_ticker, event_ticker, title,
                     yes_sub_title, no_sub_title, result, last_price,
                     volume, open_interest, open_time, close_time, expiration_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_ticker) DO UPDATE SET
                    result = EXCLUDED.result,
                    last_price = EXCLUDED.last_price,
                    volume = EXCLUDED.volume,
                    open_interest = EXCLUDED.open_interest
                """,
                rows,
                page_size=500,
            )
        conn.commit()
        log.info("Upserted %d markets", len(rows))
    finally:
        conn.close()


def get_resolved_markets(series_ticker: str) -> list[dict]:
    """Get all resolved markets for a series."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM kalshi.historical_resolutions
                WHERE series_ticker = %s AND result IS NOT NULL
                ORDER BY close_time
                """,
                (series_ticker,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_open_markets(series_ticker: str) -> list[dict]:
    """Get all open markets for a series."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM kalshi.historical_resolutions
                WHERE series_ticker = %s AND result IS NULL
                ORDER BY expiration_time
                """,
                (series_ticker,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()
