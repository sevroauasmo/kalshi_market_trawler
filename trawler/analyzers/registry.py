"""Registry mapping series tickers to analyzer classes."""
import logging

from trawler.analyzers.base import BaseAnalyzer

log = logging.getLogger(__name__)

_REGISTRY: dict[str, type[BaseAnalyzer]] = {}


def register(series_ticker: str):
    """Decorator to register an analyzer for a series ticker."""
    def decorator(cls):
        _REGISTRY[series_ticker] = cls
        return cls
    return decorator


def get_analyzer(series_ticker: str) -> BaseAnalyzer | None:
    """Get an instantiated analyzer for the given series ticker."""
    cls = _REGISTRY.get(series_ticker)
    if cls is None:
        log.warning("No analyzer registered for %s", series_ticker)
        return None
    return cls()


def list_analyzers() -> list[str]:
    """Return all registered series tickers."""
    return list(_REGISTRY.keys())


# Import analyzer modules to trigger registration
def _load_all():
    try:
        import trawler.analyzers.mta_ridership  # noqa: F401
    except ImportError:
        pass


_load_all()
