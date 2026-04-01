"""MTA subway ridership analyzer for KXSUBWAY series.

Fetches daily ridership from NY Open Data SODA API, extracts trends
and seasonal patterns, and estimates probabilities for Kalshi markets
based on ridership thresholds.
"""
import logging
import re
from pathlib import Path

import pandas as pd
import requests

from trawler.analyzers.base import BaseAnalyzer
from trawler.analyzers.registry import register
from trawler.config import DATA_DIR

log = logging.getLogger(__name__)

API_URL = "https://data.ny.gov/resource/sayj-mze2.json"
CACHE_FILE = DATA_DIR / "subway_ridership.csv"


def _parse(raw: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw)
    df = df.rename(columns={"date": "date", "count": "ridership"})
    df = df[["date", "ridership"]]
    df["date"] = pd.to_datetime(df["date"])
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce")
    df = df.dropna(subset=["ridership"]).sort_values("date").reset_index(drop=True)
    return df


def _fetch_all() -> pd.DataFrame:
    params = {
        "$limit": 50000,
        "$order": "date ASC",
        "$where": "mode='Subway'",
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return _parse(resp.json())


def _update_cache() -> pd.DataFrame:
    if CACHE_FILE.exists():
        cached = pd.read_csv(CACHE_FILE, parse_dates=["date"])
        last_date = cached["date"].max().strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            "$limit": 50000,
            "$order": "date ASC",
            "$where": f"mode='Subway' AND date > '{last_date}'",
        }
        resp = requests.get(API_URL, params=params)
        resp.raise_for_status()
        new_rows = resp.json()
        if new_rows:
            new_df = _parse(new_rows)
            df = pd.concat([cached, new_df], ignore_index=True)
        else:
            df = cached
    else:
        log.info("No cache found, fetching all ridership data...")
        df = _fetch_all()

    df = df.sort_values("date").reset_index(drop=True)
    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(CACHE_FILE, index=False)
    return df


def _extract_threshold(title: str) -> int | None:
    """Extract ridership threshold from market title.

    Titles look like:
      'Will NYC subway ridership exceed 3.5 million on ...?'
      'NYC subway daily ridership above 4M on ...?'
    """
    # Try patterns like "3.5 million", "3,500,000", "4M"
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:million|mil|M)\b', title, re.IGNORECASE)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    m = re.search(r'(\d{1,3}(?:,\d{3})+)', title)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def _extract_date(title: str) -> pd.Timestamp | None:
    """Try to extract the target date from market title."""
    # Common patterns: "on March 15, 2026", "on 3/15/2026"
    for pattern in [
        r'on\s+(\w+\s+\d{1,2},?\s+\d{4})',
        r'on\s+(\d{1,2}/\d{1,2}/\d{4})',
    ]:
        m = re.search(pattern, title, re.IGNORECASE)
        if m:
            try:
                return pd.Timestamp(m.group(1))
            except (ValueError, pd.errors.OutOfBoundsDatetime):
                pass
    return None


@register("KXSUBWAY")
class MTARidershipAnalyzer(BaseAnalyzer):
    series_ticker = "KXSUBWAY"
    edge_threshold = 0.08  # 8pp — KXSUBWAY has shown larger edges historically

    def __init__(self):
        self.data: pd.DataFrame | None = None

    def fetch_underlying_data(self) -> pd.DataFrame:
        self.data = _update_cache()
        log.info("Loaded %d ridership rows (%s to %s)",
                 len(self.data),
                 self.data["date"].min().date(),
                 self.data["date"].max().date())
        return self.data

    def estimate_probability(self, market: dict) -> float:
        if self.data is None or self.data.empty:
            return 0.5

        title = market.get("title", "") or ""
        yes_sub = market.get("yes_sub_title", "") or ""
        threshold = _extract_threshold(title) or _extract_threshold(yes_sub)
        if threshold is None:
            log.debug("Could not extract threshold from: %s / %s", title, yes_sub)
            return 0.5

        target_date = _extract_date(title)

        # Use same-day-of-week historical data for seasonal adjustment
        df = self.data.copy()
        df["dow"] = df["date"].dt.dayofweek
        df["month"] = df["date"].dt.month

        if target_date is not None:
            target_dow = target_date.dayofweek
            target_month = target_date.month
            # Filter to same day-of-week and nearby months for seasonal relevance
            nearby_months = [(target_month - 1) % 12 or 12, target_month, (target_month % 12) + 1]
            relevant = df[(df["dow"] == target_dow) & (df["month"].isin(nearby_months))]
        else:
            relevant = df

        if relevant.empty:
            relevant = df

        # Recent trend: weight last 90 days more heavily
        recent = df[df["date"] >= df["date"].max() - pd.Timedelta(days=90)]
        if not recent.empty:
            recent_mean = recent["ridership"].mean()
            recent_std = recent["ridership"].std()
        else:
            recent_mean = relevant["ridership"].mean()
            recent_std = relevant["ridership"].std()

        # Historical base rate: how often does ridership exceed threshold?
        historical_rate = (relevant["ridership"] >= threshold).mean()

        # Recent rate
        if not recent.empty:
            recent_rate = (recent["ridership"] >= threshold).mean()
        else:
            recent_rate = historical_rate

        # Blend: 60% recent, 40% historical (recency matters for trends)
        estimate = 0.6 * recent_rate + 0.4 * historical_rate

        # Clamp to avoid extreme predictions
        estimate = max(0.03, min(0.97, estimate))

        log.debug(
            "  threshold=%d | hist_rate=%.2f recent_rate=%.2f → estimate=%.2f",
            threshold, historical_rate, recent_rate, estimate,
        )
        return estimate

    def explain(self, market: dict, estimate: float) -> str:
        title = market.get("title", "") or ""
        yes_sub = market.get("yes_sub_title", "") or ""
        threshold = _extract_threshold(title) or _extract_threshold(yes_sub)
        if threshold is None:
            return f"Estimated probability: {estimate:.1%} vs market: {float(market.get('last_price', 0)):.1%}"

        df = self.data
        recent = df[df["date"] >= df["date"].max() - pd.Timedelta(days=90)]
        recent_mean = recent["ridership"].mean() if not recent.empty else 0
        recent_exceed = (recent["ridership"] >= threshold).mean() if not recent.empty else 0

        return (
            f"Threshold: {threshold:,} riders. "
            f"Recent 90-day avg: {recent_mean:,.0f}, "
            f"recent exceed rate: {recent_exceed:.0%}. "
            f"Estimated prob: {estimate:.1%} vs market: {float(market.get('last_price', 0)):.1%}"
        )
