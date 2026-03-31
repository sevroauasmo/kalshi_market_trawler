import logging
import time
from typing import Iterator

import httpx

from trawler.api.auth import load_private_key, sign_request
from trawler.api.models import Event, Market, Series
from trawler.config import KALSHI_API_BASE, KALSHI_PRIVATE_KEY_PATH, KALSHI_RATE_LIMIT

log = logging.getLogger(__name__)


def _dollars(val) -> float:
    """Convert dollar string like '0.0500' to float."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, rate: int):
        self.rate = rate
        self.tokens = rate
        self.last_refill = time.monotonic()

    def acquire(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
        self.last_refill = now
        if self.tokens < 1:
            sleep_time = (1 - self.tokens) / self.rate
            time.sleep(sleep_time)
            self.tokens = 0
        else:
            self.tokens -= 1


class KalshiClient:
    def __init__(self, authenticated: bool = False):
        self.base_url = KALSHI_API_BASE
        self.client = httpx.Client(timeout=30.0)
        self.limiter = RateLimiter(KALSHI_RATE_LIMIT)
        self.private_key = None
        if authenticated and KALSHI_PRIVATE_KEY_PATH:
            self.private_key = load_private_key(KALSHI_PRIVATE_KEY_PATH)

    def _headers(self, method: str, path: str) -> dict:
        headers = {"Accept": "application/json"}
        if self.private_key:
            ts = int(time.time() * 1000)
            sig = sign_request(self.private_key, ts, method, path)
            headers["KALSHI-ACCESS-KEY"] = KALSHI_PRIVATE_KEY_PATH
            headers["KALSHI-ACCESS-TIMESTAMP"] = str(ts)
            headers["KALSHI-ACCESS-SIGNATURE"] = sig
        return headers

    def _get(self, path: str, params: dict | None = None) -> dict:
        self.limiter.acquire()
        url = f"{self.base_url}{path}"
        headers = self._headers("GET", path)
        resp = self.client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def _paginate(self, path: str, key: str, params: dict | None = None) -> Iterator[dict]:
        """Yield all items from a paginated endpoint."""
        params = dict(params or {})
        params.setdefault("limit", 200)
        while True:
            data = self._get(path, params)
            items = data.get(key, [])
            yield from items
            cursor = data.get("cursor")
            if not cursor or not items:
                break
            params["cursor"] = cursor

    # ── Series ──────────────────────────────────────────────────────

    def get_all_series(self) -> list[Series]:
        """Fetch all series (no pagination on this endpoint)."""
        data = self._get("/series")
        series_list = data.get("series", [])
        return [self._parse_series(s) for s in series_list]

    def get_series(self, ticker: str) -> Series:
        data = self._get(f"/series/{ticker}")
        return self._parse_series(data.get("series", data))

    def _parse_series(self, raw: dict) -> Series:
        return Series(
            ticker=raw.get("ticker", ""),
            title=raw.get("title", ""),
            frequency=raw.get("frequency", ""),
            category=raw.get("category", ""),
            tags=raw.get("tags") or [],
            settlement_sources=raw.get("settlement_sources") or [],
            total_volume=raw.get("volume", 0) or 0,
        )

    # ── Markets ─────────────────────────────────────────────────────

    def get_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str | None = None,
    ) -> Iterator[Market]:
        params = {}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if status:
            params["status"] = status
        for raw in self._paginate("/markets", "markets", params):
            yield self._parse_market(raw, series_ticker=series_ticker or "")

    def _parse_market(self, raw: dict, series_ticker: str = "") -> Market:
        # API doesn't return series_ticker on markets; infer from query context
        result = raw.get("result", "")
        return Market(
            ticker=raw.get("ticker", ""),
            event_ticker=raw.get("event_ticker", ""),
            series_ticker=series_ticker,
            title=raw.get("title", ""),
            yes_sub_title=raw.get("yes_sub_title", ""),
            no_sub_title=raw.get("no_sub_title", ""),
            status=raw.get("status", ""),
            result=result if result else None,
            yes_bid=_dollars(raw.get("yes_bid_dollars", "0")),
            yes_ask=_dollars(raw.get("yes_ask_dollars", "0")),
            last_price=_dollars(raw.get("last_price_dollars", "0")),
            volume=float(raw.get("volume_fp", 0) or 0),
            open_interest=float(raw.get("open_interest_fp", 0) or 0),
            open_time=raw.get("open_time", ""),
            close_time=raw.get("close_time", ""),
            expiration_time=raw.get("expiration_time", raw.get("latest_expiration_time", "")),
        )

    # ── Events ──────────────────────────────────────────────────────

    def get_events(self, series_ticker: str) -> Iterator[Event]:
        params = {"series_ticker": series_ticker, "with_nested_markets": "true"}
        for raw in self._paginate("/events", "events", params):
            markets = [self._parse_market(m, series_ticker=series_ticker) for m in (raw.get("markets") or [])]
            yield Event(
                ticker=raw.get("event_ticker", ""),
                series_ticker=raw.get("series_ticker", ""),
                title=raw.get("title", ""),
                category=raw.get("category", ""),
                markets=markets,
            )

    # ── Candlesticks ────────────────────────────────────────────────

    def get_candlesticks(self, series_ticker: str, market_ticker: str) -> list[dict]:
        """Get price history for a market. Returns raw candlestick dicts."""
        data = self._get(
            "/series/{}/markets/{}/candlesticks".format(series_ticker, market_ticker)
        )
        return data.get("candlesticks", [])

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
