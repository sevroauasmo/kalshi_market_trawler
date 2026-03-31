"""Calibration analysis for candidate series.

For each series with enough resolved markets, compute how well the market
prices predicted outcomes. Large systematic miscalibration = opportunity.
"""
import json
import logging
from datetime import datetime, timezone

from trawler.db.connection import get_connection

log = logging.getLogger(__name__)

MIN_RESOLVED = 5  # Need at least this many resolved markets
BUCKETS = [(i / 10, (i + 1) / 10) for i in range(10)]  # 0-10%, 10-20%, ..., 90-100%


def _bucket_label(low: float, high: float) -> str:
    return f"{int(low * 100)}-{int(high * 100)}¢"


def compute_calibration(series_ticker: str) -> dict | None:
    """Compute calibration for a single series. Returns None if insufficient data."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_price, result
                FROM kalshi.historical_resolutions
                WHERE series_ticker = %s AND result IS NOT NULL AND last_price IS NOT NULL
                """,
                (series_ticker,),
            )
            rows = cur.fetchall()

        if len(rows) < MIN_RESOLVED:
            return None

        # Build calibration curve
        curve = {}
        for low, high in BUCKETS:
            bucket_markets = [
                (price, result)
                for price, result in rows
                if low <= float(price) < high
            ]
            if not bucket_markets:
                continue
            label = _bucket_label(low, high)
            hit_rate = sum(1 for _, r in bucket_markets if r == "yes") / len(bucket_markets)
            avg_price = sum(float(p) for p, _ in bucket_markets) / len(bucket_markets)
            curve[label] = {
                "count": len(bucket_markets),
                "avg_price": round(avg_price, 3),
                "actual_hit_rate": round(hit_rate, 3),
                "calibration_error": round(abs(hit_rate - avg_price), 3),
            }

        if not curve:
            return None

        # Overall stats
        total_error = sum(b["calibration_error"] * b["count"] for b in curve.values())
        total_count = sum(b["count"] for b in curve.values())
        avg_error = total_error / total_count if total_count else 0

        worst = max(curve.items(), key=lambda x: x[1]["calibration_error"])
        worst_label = f"{worst[0]} markets resolve yes {worst[1]['actual_hit_rate']:.0%} of the time"

        return {
            "series_ticker": series_ticker,
            "total_markets_resolved": len(rows),
            "avg_calibration_error": round(avg_error, 3),
            "worst_bucket": worst_label,
            "calibration_curve": curve,
        }
    finally:
        conn.close()


def save_calibration(cal: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO kalshi.calibration_scores
                    (series_ticker, total_markets_resolved, avg_calibration_error,
                     worst_bucket, calibration_curve, last_computed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_ticker) DO UPDATE SET
                    total_markets_resolved = EXCLUDED.total_markets_resolved,
                    avg_calibration_error = EXCLUDED.avg_calibration_error,
                    worst_bucket = EXCLUDED.worst_bucket,
                    calibration_curve = EXCLUDED.calibration_curve,
                    last_computed_at = EXCLUDED.last_computed_at
                """,
                (
                    cal["series_ticker"],
                    cal["total_markets_resolved"],
                    cal["avg_calibration_error"],
                    cal["worst_bucket"],
                    json.dumps(cal["calibration_curve"]),
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def compute_all():
    """Compute calibration for all candidate series with enough data."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Get series that have resolved markets
            cur.execute("""
                SELECT DISTINCT h.series_ticker
                FROM kalshi.historical_resolutions h
                JOIN kalshi.series_catalog s ON s.ticker = h.series_ticker
                WHERE h.result IS NOT NULL
                GROUP BY h.series_ticker
                HAVING COUNT(*) >= %s
            """, (MIN_RESOLVED,))
            tickers = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    log.info("Computing calibration for %d series with >= %d resolved markets...", len(tickers), MIN_RESOLVED)
    computed = 0
    for ticker in tickers:
        cal = compute_calibration(ticker)
        if cal:
            save_calibration(cal)
            computed += 1
            if cal["avg_calibration_error"] > 0.15:
                log.info(
                    "  %s: avg_error=%.1f%% (%d markets) — %s",
                    ticker,
                    cal["avg_calibration_error"] * 100,
                    cal["total_markets_resolved"],
                    cal["worst_bucket"],
                )

    log.info("Computed calibration for %d series", computed)
