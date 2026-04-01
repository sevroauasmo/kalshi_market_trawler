"""Base interface for per-series market analyzers."""
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import pandas as pd

from trawler.db.connection import get_connection
from trawler.db.resolutions_repo import get_open_markets

log = logging.getLogger(__name__)


@dataclass
class Opportunity:
    market_ticker: str
    series_ticker: str
    market_title: str
    market_price: float       # what Kalshi says (0-1)
    our_estimate: float       # what we think (0-1)
    edge: float               # difference (positive = we think yes is underpriced)
    confidence: str           # "high", "medium", "low"
    reasoning: str
    underlying_data: dict
    recommended_side: str     # "yes" or "no"
    recommended_position: float  # dollars


class BaseAnalyzer(ABC):
    """Base class for per-series analysis modules.

    Subclasses must implement:
        - series_ticker: str
        - fetch_underlying_data() -> pd.DataFrame
        - estimate_probability(market: dict) -> float
    """

    series_ticker: str
    edge_threshold: float = 0.10  # minimum edge to flag (10pp)

    @abstractmethod
    def fetch_underlying_data(self) -> pd.DataFrame:
        """Pull the public data this market resolves against."""
        ...

    @abstractmethod
    def estimate_probability(self, market: dict) -> float:
        """Estimate true probability of 'yes' resolution. Returns 0-1."""
        ...

    def compute_confidence(self, market: dict, estimate: float) -> str:
        """Override to provide custom confidence logic."""
        edge = abs(estimate - float(market.get("last_price", 0) or 0))
        if edge > 0.30:
            return "high"
        if edge > 0.15:
            return "medium"
        return "low"

    def compute_position_size(self, edge: float, confidence: str) -> float:
        """Suggested position size in dollars. Override for custom sizing."""
        base = {"high": 300, "medium": 150, "low": 50}
        return base.get(confidence, 50)

    def compare_to_market(self, market: dict) -> Opportunity | None:
        """Compare our estimate to market pricing. Returns Opportunity or None."""
        market_price = float(market.get("last_price", 0) or 0)
        if market_price <= 0:
            return None

        estimate = float(self.estimate_probability(market))
        edge = estimate - market_price
        abs_edge = abs(edge)

        if abs_edge < self.edge_threshold:
            return None

        confidence = self.compute_confidence(market, estimate)
        side = "yes" if edge > 0 else "no"
        position = self.compute_position_size(abs_edge, confidence)

        return Opportunity(
            market_ticker=market["market_ticker"],
            series_ticker=self.series_ticker,
            market_title=market.get("title", ""),
            market_price=market_price,
            our_estimate=round(estimate, 3),
            edge=round(edge, 3),
            confidence=confidence,
            reasoning=self.explain(market, estimate),
            underlying_data={},
            recommended_side=side,
            recommended_position=position,
        )

    def explain(self, market: dict, estimate: float) -> str:
        """Override to provide human-readable reasoning."""
        return f"Estimated probability: {estimate:.1%} vs market: {float(market.get('last_price', 0)):.1%}"

    def find_opportunities(self) -> list[Opportunity]:
        """Scan open markets for this series and return opportunities."""
        log.info("Fetching underlying data for %s...", self.series_ticker)
        self.fetch_underlying_data()

        open_markets = get_open_markets(self.series_ticker)
        log.info("Found %d open markets for %s", len(open_markets), self.series_ticker)

        opportunities = []
        for market in open_markets:
            try:
                opp = self.compare_to_market(market)
                if opp:
                    opportunities.append(opp)
                    log.info(
                        "  OPPORTUNITY: %s | price=%.0f¢ est=%.0f¢ edge=%+.0fpp | %s",
                        opp.market_ticker,
                        opp.market_price * 100,
                        opp.our_estimate * 100,
                        opp.edge * 100,
                        opp.confidence,
                    )
            except Exception as e:
                log.warning("Error analyzing %s: %s", market.get("market_ticker"), e)

        if opportunities:
            self._save_opportunities(opportunities)

        return opportunities

    def _save_opportunities(self, opportunities: list[Opportunity]):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                for opp in opportunities:
                    cur.execute(
                        """
                        INSERT INTO kalshi.opportunities
                            (market_ticker, series_ticker, market_title, market_price,
                             our_estimate, edge, confidence, reasoning, underlying_data,
                             recommended_side, recommended_position, detected_at, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'open')
                        ON CONFLICT (market_ticker) DO UPDATE SET
                            market_price = EXCLUDED.market_price,
                            our_estimate = EXCLUDED.our_estimate,
                            edge = EXCLUDED.edge,
                            confidence = EXCLUDED.confidence,
                            reasoning = EXCLUDED.reasoning,
                            underlying_data = EXCLUDED.underlying_data,
                            recommended_side = EXCLUDED.recommended_side,
                            recommended_position = EXCLUDED.recommended_position,
                            detected_at = EXCLUDED.detected_at
                        """,
                        (
                            opp.market_ticker,
                            opp.series_ticker,
                            opp.market_title,
                            opp.market_price,
                            opp.our_estimate,
                            opp.edge,
                            opp.confidence,
                            opp.reasoning,
                            json.dumps(opp.underlying_data),
                            opp.recommended_side,
                            opp.recommended_position,
                            datetime.now(timezone.utc),
                        ),
                    )
            conn.commit()
            log.info("Saved %d opportunities to DB", len(opportunities))
        finally:
            conn.close()
