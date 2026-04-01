"""Pull early trading prices from Kalshi candlestick data.

The `last_price` in historical_resolutions is typically the settlement price
(0.99 or 0.01), not a tradeable price.  This module fetches hourly candlestick
data and records the volume-weighted mean price from the first hour or two of
trading -- a much better proxy for the price you could have actually traded at.
"""

import logging
from datetime import datetime, timedelta, timezone

from trawler.api.client import KalshiClient
from trawler.db.connection import get_connection

log = logging.getLogger(__name__)

# Series we care about most, in priority order
PRIORITY_SERIES = [
    "KXHIGHNY",
    "KXHIGHCHI",
    "KXHIGHMIA",
    "KXHIGHAUS",
    "KXHIGHDEN",
    "KXHIGHLAX",
    "KXECONSTATCPIYOY",
    "KXECONSTATCPICORE",
    "KXCPIYOY",
    "KXCPICOREYOY",
    "KXAAAGASW",
    "KXAAAGASM",
]


def _extract_early_price(candles: list[dict]) -> tuple[float | None, datetime | None]:
    """Pick the best early-trading price from candlestick data.

    Strategy: use the candle roughly 1 hour after open_time (first or second
    candle).  Prefer ``price.mean_dollars`` (VWAP for that hour), falling back
    to ``price.close_dollars`` of the first candle.

    Returns (early_price, early_price_time) or (None, None).
    """
    if not candles:
        return None, None

    # Sort by timestamp ascending
    candles = sorted(candles, key=lambda c: c.get("end_period_ts", 0))

    # Try the second candle first (approx 1 hour after open), fall back to first
    for idx in (1, 0):
        if idx >= len(candles):
            continue
        candle = candles[idx]
        price_data = candle.get("price", {})

        mean_str = price_data.get("mean_dollars")
        close_str = price_data.get("close_dollars")

        price_val = None
        if mean_str:
            try:
                price_val = float(mean_str)
            except (ValueError, TypeError):
                pass

        # Fall back to close
        if price_val is None and close_str:
            try:
                price_val = float(close_str)
            except (ValueError, TypeError):
                pass

        if price_val is not None and price_val > 0:
            ts = candle.get("end_period_ts")
            price_time = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
            return price_val, price_time

    return None, None


def pull_early_prices(series_ticker: str | None = None, limit: int | None = None):
    """Pull early trading prices for resolved markets.

    If *series_ticker* is given, only process that series.
    Otherwise process all :data:`PRIORITY_SERIES` in order.
    """
    series_list = [series_ticker] if series_ticker else PRIORITY_SERIES

    conn = get_connection()
    client = KalshiClient(authenticated=True)

    total_processed = 0
    total_updated = 0
    total_no_data = 0

    try:
        for sticker in series_list:
            log.info("Processing series: %s", sticker)

            # Fetch resolved markets that still need an early_price
            with conn.cursor() as cur:
                query = """
                    SELECT market_ticker, open_time, close_time
                    FROM kalshi.historical_resolutions
                    WHERE series_ticker = %s
                      AND result IS NOT NULL
                      AND early_price IS NULL
                    ORDER BY open_time DESC
                """
                params: list = [sticker]
                if limit is not None:
                    remaining = limit - total_processed
                    if remaining <= 0:
                        break
                    query += " LIMIT %s"
                    params.append(remaining)
                cur.execute(query, params)
                markets = cur.fetchall()

            if not markets:
                log.info("  No markets needing early_price for %s", sticker)
                continue

            log.info("  Found %d markets to process for %s", len(markets), sticker)

            series_updated = 0
            for market_ticker, open_time, close_time in markets:
                total_processed += 1

                # Compute start_ts / end_ts for the candlestick window.
                # We only need the first couple of hours after the market opened.
                if open_time is not None:
                    if open_time.tzinfo is None:
                        open_time = open_time.replace(tzinfo=timezone.utc)
                    start_ts = int(open_time.timestamp())
                    end_ts = int((open_time + timedelta(hours=3)).timestamp())
                else:
                    # No open_time -- try a wide window ending at close
                    if close_time is not None:
                        if close_time.tzinfo is None:
                            close_time = close_time.replace(tzinfo=timezone.utc)
                        end_ts = int(close_time.timestamp())
                        start_ts = end_ts - 3 * 3600
                    else:
                        total_no_data += 1
                        continue

                try:
                    candles = client.get_candlesticks(
                        sticker, market_ticker,
                        start_ts=start_ts, end_ts=end_ts,
                    )
                except Exception as exc:
                    log.warning("  Failed to fetch candles for %s: %s", market_ticker, exc)
                    total_no_data += 1
                    continue

                early_price, early_price_time = _extract_early_price(candles)

                if early_price is None:
                    total_no_data += 1
                    if total_no_data <= 5:
                        log.debug("  No candle data for %s", market_ticker)
                    continue

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE kalshi.historical_resolutions
                        SET early_price = %s, early_price_time = %s
                        WHERE market_ticker = %s
                        """,
                        (early_price, early_price_time, market_ticker),
                    )
                conn.commit()
                total_updated += 1
                series_updated += 1

                if total_updated <= 5 or total_updated % 50 == 0:
                    log.info(
                        "  [%d/%d] %s  early_price=%.4f  time=%s",
                        total_updated,
                        total_processed,
                        market_ticker,
                        early_price,
                        early_price_time,
                    )

            log.info(
                "  Finished %s: %d updated out of %d processed this series",
                sticker, series_updated, len(markets),
            )

        # Print summary
        _print_summary(conn, series_list, total_processed, total_updated, total_no_data)

    finally:
        client.close()
        conn.close()


def _print_summary(conn, series_list: list[str], processed: int, updated: int, no_data: int):
    """Print a summary of early price distribution."""
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Markets processed: %d", processed)
    log.info("  Markets updated:   %d", updated)
    log.info("  No candle data:    %d", no_data)

    # Price distribution for series we just processed
    placeholders = ",".join(["%s"] * len(series_list))
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(early_price) AS with_price,
                    ROUND(AVG(early_price)::numeric, 4) AS avg_price,
                    ROUND(MIN(early_price)::numeric, 4) AS min_price,
                    ROUND(MAX(early_price)::numeric, 4) AS max_price,
                    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY early_price)::numeric, 4) AS p25,
                    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY early_price)::numeric, 4) AS p50,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY early_price)::numeric, 4) AS p75
                FROM kalshi.historical_resolutions
                WHERE series_ticker IN ({placeholders})
                  AND early_price IS NOT NULL
                """,
                series_list,
            )
            row = cur.fetchone()
            if row and row[0]:
                total, with_price, avg_p, min_p, max_p, p25, p50, p75 = row
                log.info("  Early price distribution (across %d markets with data):", with_price)
                log.info("    Min=%.4f  P25=%.4f  Median=%.4f  P75=%.4f  Max=%.4f  Avg=%.4f",
                         min_p, p25, p50, p75, max_p, avg_p)

            # Compare early_price vs last_price
            cur.execute(
                f"""
                SELECT
                    ROUND(AVG(last_price)::numeric, 4) AS avg_last,
                    ROUND(AVG(early_price)::numeric, 4) AS avg_early,
                    ROUND(AVG(ABS(last_price - early_price))::numeric, 4) AS avg_diff
                FROM kalshi.historical_resolutions
                WHERE series_ticker IN ({placeholders})
                  AND early_price IS NOT NULL
                  AND last_price IS NOT NULL
                """,
                series_list,
            )
            row = cur.fetchone()
            if row and row[0]:
                avg_last, avg_early, avg_diff = row
                log.info("  last_price avg=%.4f  early_price avg=%.4f  avg |diff|=%.4f",
                         avg_last, avg_early, avg_diff)
    except Exception as exc:
        log.warning("Could not compute summary stats: %s", exc)
    log.info("=" * 60)
