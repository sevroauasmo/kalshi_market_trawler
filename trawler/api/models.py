from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Series:
    ticker: str
    title: str = ""
    frequency: str = ""
    category: str = ""
    tags: list[str] = field(default_factory=list)
    settlement_sources: list[dict] = field(default_factory=list)
    total_volume: float = 0.0


@dataclass
class Market:
    ticker: str
    event_ticker: str = ""
    series_ticker: str = ""
    title: str = ""
    yes_sub_title: str = ""
    no_sub_title: str = ""
    status: str = ""
    result: str | None = None
    yes_bid: float = 0.0
    yes_ask: float = 0.0
    last_price: float = 0.0
    volume: float = 0.0
    open_interest: float = 0.0
    open_time: str = ""
    close_time: str = ""
    expiration_time: str = ""


@dataclass
class Event:
    ticker: str
    series_ticker: str = ""
    title: str = ""
    category: str = ""
    markets: list[Market] = field(default_factory=list)
