import json
import logging
from datetime import datetime, timezone

import psycopg2.extras

from trawler.api.models import Series
from trawler.db.connection import get_connection

log = logging.getLogger(__name__)


def upsert_series(series_list: list[Series]):
    """Upsert a batch of series into kalshi.series_catalog."""
    conn = get_connection()
    try:
        rows = [
            (
                s.ticker,
                s.title,
                s.frequency,
                s.category,
                s.tags or [],
                json.dumps(s.settlement_sources) if s.settlement_sources else None,
                s.total_volume,
                datetime.now(timezone.utc),
            )
            for s in series_list
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO kalshi.series_catalog
                    (ticker, title, frequency, category, tags, settlement_sources,
                     total_volume, last_scanned_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                    title = EXCLUDED.title,
                    frequency = EXCLUDED.frequency,
                    category = EXCLUDED.category,
                    tags = EXCLUDED.tags,
                    settlement_sources = EXCLUDED.settlement_sources,
                    total_volume = EXCLUDED.total_volume,
                    last_scanned_at = EXCLUDED.last_scanned_at
                """,
                rows,
                page_size=500,
            )
        conn.commit()
        log.info("Upserted %d series", len(rows))
    finally:
        conn.close()


def get_all_series_tickers() -> list[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM kalshi.series_catalog ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def get_candidate_tickers() -> list[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker FROM kalshi.series_catalog WHERE candidate_status = 'candidate' ORDER BY ticker"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def update_candidate_status(ticker: str, status: str, reason: str | None = None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE kalshi.series_catalog
                SET candidate_status = %s, rejection_reason = %s
                WHERE ticker = %s
                """,
                (status, reason, ticker),
            )
        conn.commit()
    finally:
        conn.close()
