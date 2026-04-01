"""Candidate filtering heuristics for Kalshi series.

Applies four filters to narrow the universe:
1. Recurring — frequency is not null/empty/one_off
2. Accessible settlement source — gov data, public APIs, structured data
3. Retail-viable volume — aggregate market volume in sweet spot
4. Historical mispricing signal — (requires calibration data, applied separately)
"""
import json
import logging

import psycopg2.extras

from trawler.db.connection import get_connection

log = logging.getLogger(__name__)

# Settlement source domains/keywords that indicate programmatic access
ACCESSIBLE_PATTERNS = [
    # Government data
    "data.gov", "bls.gov", "census.gov", "noaa.gov", "weather.gov",
    "eia.gov", "fred.stlouisfed.org", "treasury.gov", "data.ny.gov",
    "sec.gov", "usda.gov", "cdc.gov", "epa.gov",
    # Weather
    "openweathermap", "weather.com", "accuweather",
    # Financial
    "yahoo.com/finance", "finance.yahoo", "google.com/finance",
    "tradingview", "coinmarketcap", "coingecko",
    # Sports / structured
    "espn.com", "nfl.com", "nba.com", "mlb.com",
    "basketball-reference", "pro-football-reference",
    # Other structured
    "wikipedia.org", "imdb.com", "rottentomatoes.com",
    "metacritic.com", "boxofficemojo", "the-numbers.com",
    "youtube.com", "x.com", "twitter.com",
    "gasbuddy.com",
]

# Categories that tend to have modelable underlying data
PROMISING_CATEGORIES = {
    "Economics", "Climate and Weather", "Financial", "Energy",
    "Transportation", "Companies", "Tech",
}

# Frequency values that indicate recurring markets
RECURRING_FREQUENCIES = {"daily", "weekly", "monthly", "quarterly", "yearly", "custom"}
NON_RECURRING = {"one_off", ""}

# Volume thresholds (aggregate across all markets in the series)
MIN_SERIES_VOLUME = 5_000    # $5k minimum (lowered from spec to catch emerging markets)
MAX_SERIES_VOLUME = 5_000_000  # $5M cap


def _has_accessible_source(settlement_sources) -> bool:
    """Check if any settlement source URL matches known accessible patterns."""
    if not settlement_sources:
        return False
    if isinstance(settlement_sources, str):
        try:
            settlement_sources = json.loads(settlement_sources)
        except (json.JSONDecodeError, TypeError):
            return False
    for source in settlement_sources:
        url = (source.get("url") or "").lower()
        for pattern in ACCESSIBLE_PATTERNS:
            if pattern in url:
                return True
    return False


def _classify_source(settlement_sources) -> str:
    """Return a human-readable classification of the settlement source."""
    if not settlement_sources:
        return "none"
    if isinstance(settlement_sources, str):
        try:
            settlement_sources = json.loads(settlement_sources)
        except (json.JSONDecodeError, TypeError):
            return "unknown"
    urls = [s.get("url", "") for s in settlement_sources]
    url_str = " ".join(urls).lower()
    if any(p in url_str for p in ["data.gov", "bls.gov", "census.gov", "noaa.gov",
                                   "weather.gov", "eia.gov", "fred.", "treasury.gov",
                                   "data.ny.gov", "sec.gov", "usda.gov", "cdc.gov"]):
        return "government"
    if any(p in url_str for p in ["espn", "nfl.com", "nba.com", "mlb.com",
                                   "basketball-reference", "pro-football-reference"]):
        return "sports"
    if any(p in url_str for p in ["yahoo.com/finance", "finance.yahoo", "tradingview",
                                   "coinmarketcap", "coingecko"]):
        return "financial"
    if any(p in url_str for p in ["openweathermap", "weather.com", "accuweather"]):
        return "weather"
    if any(p in url_str for p in ["rottentomatoes", "metacritic", "imdb", "boxofficemojo"]):
        return "entertainment"
    if _has_accessible_source(settlement_sources):
        return "other_accessible"
    return "unknown"


def run_filters():
    """Apply all filters to series_catalog and update candidate_status."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Get all unscreened or re-screenable series
            cur.execute("""
                SELECT s.ticker, s.frequency, s.category, s.settlement_sources,
                       COALESCE(agg.total_vol, 0) as total_vol,
                       COALESCE(agg.market_count, 0) as market_count
                FROM kalshi.series_catalog s
                LEFT JOIN (
                    SELECT series_ticker,
                           SUM(volume) as total_vol,
                           COUNT(*) as market_count
                    FROM kalshi.historical_resolutions
                    GROUP BY series_ticker
                ) agg ON agg.series_ticker = s.ticker
            """)
            rows = cur.fetchall()

        log.info("Screening %d series...", len(rows))
        candidates = []
        rejections = []  # (ticker, reason) pairs

        for ticker, frequency, category, settlement_sources, total_vol, market_count in rows:
            # Filter 1: Recurring
            if not frequency or frequency in NON_RECURRING:
                rejections.append((ticker, "not_recurring"))
                continue

            # Filter 2: Accessible settlement source
            if not _has_accessible_source(settlement_sources):
                rejections.append((ticker, "no_accessible_source"))
                continue

            # Filter 3: Volume check (only if we have market data)
            if market_count > 0 and total_vol > MAX_SERIES_VOLUME:
                rejections.append((ticker, f"volume_too_high:{total_vol:.0f}"))
                continue

            # Passed all filters
            source_type = _classify_source(settlement_sources)
            candidates.append((ticker, category, source_type, total_vol, market_count))

        # Batch update rejections
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                UPDATE kalshi.series_catalog
                SET candidate_status = 'rejected', rejection_reason = %s
                WHERE ticker = %s
                """,
                [(reason, ticker) for ticker, reason in rejections],
                page_size=500,
            )
        conn.commit()

        # Batch update candidates
        candidate_tickers = [(t,) for t, _, _, _, _ in candidates]
        if candidate_tickers:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    UPDATE kalshi.series_catalog
                    SET candidate_status = 'candidate',
                        rejection_reason = NULL
                    WHERE ticker = %s
                    """,
                    candidate_tickers,
                    page_size=500,
                )
            conn.commit()

        log.info(
            "Filtering complete: %d candidates, %d rejected out of %d total",
            len(candidates), len(rejections), len(rows),
        )

        # Log summary by source type
        from collections import Counter
        source_counts = Counter(s for _, _, s, _, _ in candidates)
        for source_type, count in source_counts.most_common():
            log.info("  %s: %d candidates", source_type, count)

    finally:
        conn.close()
