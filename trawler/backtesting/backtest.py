"""
Backtesting module — test whether public data can beat Kalshi market prices.

No-lookahead backtest: for each resolved market, the model only uses data
that was publicly available BEFORE that market's close_time.
"""

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import requests
from psycopg2.extras import execute_batch
from rich.console import Console
from rich.table import Table
from scipy import stats

from trawler.config import DATA_DIR
from trawler.db.connection import get_connection

log = logging.getLogger(__name__)
console = Console()

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
CACHE_DIR = DATA_DIR / "fred_cache"

# ──────────────────────────────────────────────────────────────────────
# FRED data fetcher with local caching (uses public CSV endpoint, no key)
# ──────────────────────────────────────────────────────────────────────

def _ensure_cache_dir():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_fred_series(series_id: str, force: bool = False) -> list[dict]:
    """Fetch a FRED series via public CSV endpoint, caching to disk."""
    _ensure_cache_dir()
    cache_path = CACHE_DIR / f"{series_id}.json"

    if cache_path.exists() and not force:
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours < 24:
            with open(cache_path) as f:
                return json.load(f)

    log.info(f"Fetching FRED series {series_id}...")
    params = {"id": series_id, "cosd": "2014-01-01"}
    resp = requests.get(FRED_CSV_URL, params=params, timeout=30)
    resp.raise_for_status()

    observations = []
    for line in resp.text.strip().split("\n")[1:]:  # skip header
        parts = line.strip().split(",")
        if len(parts) != 2 or parts[1] == ".":
            continue
        try:
            observations.append({"date": parts[0], "value": float(parts[1])})
        except ValueError:
            continue

    with open(cache_path, "w") as f:
        json.dump(observations, f)

    time.sleep(1)  # rate-limit courtesy
    return observations


# Publication lags: how many days after the observation date the data is
# actually released to the public.  FRED observation dates represent the
# *period* the data covers, NOT when it was published.
#
#   CPI (CPIAUCSL, CPILFESL): observation dated 1st of month M, released
#       around the 10th-15th of month M+1 → ~45 days after observation date.
#   Gas prices (GASREGW): weekly EIA data, ~10-day lag.
#   Fed funds rate (DFEDTARU): daily, released next business day.
FRED_PUBLICATION_LAGS = {
    "CPIAUCSL": 45,   # monthly CPI: released ~45 days after obs date (M-01)
    "CPILFESL": 45,   # core CPI: same BLS release as headline CPI
    "GASREGW": 10,    # EIA weekly gas prices: ~10-day lag
    "DFEDTARU": 1,    # fed funds rate: next business day
}

DEFAULT_PUBLICATION_LAG = 30  # conservative fallback


def get_fred_before(series_id: str, before: datetime) -> list[dict]:
    """Return FRED observations that were *publicly available* before `before`.

    Each observation's availability is estimated as:
        available_date = observation_date + publication_lag
    Only observations where available_date < before are included.
    """
    all_obs = fetch_fred_series(series_id)
    lag_days = FRED_PUBLICATION_LAGS.get(series_id, DEFAULT_PUBLICATION_LAG)
    lag = timedelta(days=lag_days)

    result = []
    for o in all_obs:
        obs_date = datetime.strptime(o["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        available_date = obs_date + lag
        if available_date < before:
            result.append(o)
    return result


# ──────────────────────────────────────────────────────────────────────
# Market data loading
# ──────────────────────────────────────────────────────────────────────

SUPPORTED_SERIES = [
    "KXECONSTATCPIYOY",
    "KXECONSTATCPICORE",
    "KXAAAGASW",
    "KXAAAGASM",
    "KXAAAGASD",
    "KXCPICOREA",
    "KXFED",
]


def load_resolved_markets(series_ticker: Optional[str] = None) -> list[dict]:
    """Load resolved markets from the database."""
    conn = get_connection()
    cur = conn.cursor()
    if series_ticker:
        tickers = [series_ticker]
    else:
        tickers = SUPPORTED_SERIES

    placeholders = ",".join(["%s"] * len(tickers))
    cur.execute(
        f"""SELECT market_ticker, series_ticker, title, yes_sub_title,
                   result, early_price, close_time
            FROM kalshi.historical_resolutions
            WHERE series_ticker IN ({placeholders})
              AND result IS NOT NULL
              AND early_price IS NOT NULL
            ORDER BY close_time""",
        tickers,
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    return rows


# ──────────────────────────────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────────────────────────────

def parse_exactly_pct(sub_title: str) -> Optional[float]:
    """Parse 'Exactly 2.7%' -> 2.7"""
    m = re.search(r"Exactly\s+([\d.]+)%", sub_title)
    return float(m.group(1)) if m else None


def parse_above_value(sub_title: str) -> Optional[float]:
    """Parse 'Above $3.970' or 'Above 3.970' or 'Above 4.25%' -> numeric value."""
    m = re.search(r"Above\s+\$?([\d.]+)%?", sub_title)
    return float(m.group(1)) if m else None


def parse_range_pct(sub_title: str) -> Optional[tuple[float, float]]:
    """Parse '2.6% to 3%' -> (2.6, 3.0) or '6.1% or above' -> (6.1, inf)
    or '0.5% or below' -> (-inf, 0.5)"""
    m = re.search(r"([\d.]+)%\s+to\s+([\d.]+)%", sub_title)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    m = re.search(r"([\d.]+)%\s+or\s+above", sub_title)
    if m:
        return (float(m.group(1)), float("inf"))
    m = re.search(r"([\d.]+)%\s+or\s+below", sub_title)
    if m:
        return (float("-inf"), float(m.group(1)))
    return None


def parse_target_month_year(title: str) -> Optional[tuple[int, int]]:
    """Parse 'CPI year-over-year in Dec 2025?' -> (12, 2025)"""
    months = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    m = re.search(r"in\s+(\w+)\s+(\d{4})", title, re.IGNORECASE)
    if m:
        mon_str = m.group(1)[:3].lower()
        if mon_str in months:
            return (months[mon_str], int(m.group(2)))
    return None


def parse_target_year(title: str) -> Optional[int]:
    """Parse year from title like 'coreinflation in 2025'."""
    m = re.search(r"in\s+(\d{4})", title)
    return int(m.group(1)) if m else None


# ──────────────────────────────────────────────────────────────────────
# Prediction logic — NO LOOKAHEAD
# ──────────────────────────────────────────────────────────────────────

def predict_cpi_yoy(market: dict) -> Optional[float]:
    """
    Predict probability that CPI YoY for a given month rounds to the target.
    Uses CPIAUCSL data available before close_time.

    Strategy: compute historical distribution of month-to-month CPI changes,
    then simulate what the target month's YoY could be.
    """
    target = parse_exactly_pct(market["yes_sub_title"])
    if target is None:
        return None

    close_time = market["close_time"]
    obs = get_fred_before("CPIAUCSL", close_time)
    if len(obs) < 24:
        return None

    # Build list of (date, value) and compute YoY and MoM changes
    values = [(o["date"], o["value"]) for o in obs]

    # The latest observation is the most recent CPI reading available
    # We need to figure out what month this market is about
    target_my = parse_target_month_year(market["title"])
    if target_my is None:
        return None
    target_month, target_year = target_my

    # Find the CPI value for target_month - 12 (same month, prior year)
    # and the most recent CPI reading
    target_date_str = f"{target_year}-{target_month:02d}-01"
    prior_year_date_str = f"{target_year - 1}-{target_month:02d}-01"

    # Get the value 12 months prior
    prior_year_val = None
    for d, v in values:
        if d == prior_year_date_str:
            prior_year_val = v
            break

    if prior_year_val is None:
        return None

    # Check if we already have the target month's CPI (it would be available
    # if FRED released it before close_time)
    target_val = None
    for d, v in values:
        if d == target_date_str:
            target_val = v
            break

    if target_val is not None:
        # We have the actual data -- compute exact YoY
        yoy = round((target_val / prior_year_val - 1) * 100, 1)
        # If actual data is already available, probability is 0 or 1
        return 1.0 if yoy == target else 0.0

    # We don't have the target month's CPI yet.
    # Use the most recent CPI value and estimate what the target month could be.
    latest_val = values[-1][1]
    latest_date = values[-1][0]

    # How many months ahead do we need to project?
    latest_year = int(latest_date[:4])
    latest_month = int(latest_date[5:7])
    months_ahead = (target_year - latest_year) * 12 + (target_month - latest_month)

    if months_ahead < 0:
        return None
    if months_ahead == 0:
        # We have the latest month = target month, shouldn't happen since
        # we checked above, but just in case
        yoy = round((latest_val / prior_year_val - 1) * 100, 1)
        return 1.0 if yoy == target else 0.0

    # Compute historical monthly changes (MoM growth rates) for the same
    # calendar month to capture seasonality
    mom_changes = []
    for i in range(12, len(values)):
        curr_date = values[i][0]
        curr_month_num = int(curr_date[5:7])
        prev_val = values[i - 1][1]
        curr_val = values[i][1]
        if prev_val > 0:
            mom_changes.append((curr_val / prev_val - 1) * 100)

    if len(mom_changes) < 24:
        return None

    # For multi-step projection, we use the distribution of MoM changes
    # to simulate forward
    mom_mean = sum(mom_changes) / len(mom_changes)
    mom_std = (sum((x - mom_mean) ** 2 for x in mom_changes) / len(mom_changes)) ** 0.5

    if mom_std < 0.001:
        mom_std = 0.1  # floor

    # The projected CPI for the target month uses months_ahead draws.
    # For a single step: projected = latest_val * (1 + mom/100)
    # YoY = (projected / prior_year_val - 1) * 100
    # We need P(round(YoY, 1) == target)

    # For months_ahead steps, the cumulative growth is approximately normal:
    # E[cum_growth] = months_ahead * mom_mean
    # Std[cum_growth] = sqrt(months_ahead) * mom_std

    # projected_val = latest_val * product(1 + mom_i/100)
    # ln(projected_val) ~ ln(latest_val) + months_ahead * E[mom/100] + noise
    # Use log-normal approximation:

    log_latest = _safe_log(latest_val)
    log_prior = _safe_log(prior_year_val)

    # Monthly log-returns
    log_returns = []
    for i in range(1, len(values)):
        if values[i][1] > 0 and values[i - 1][1] > 0:
            log_returns.append(_safe_log(values[i][1]) - _safe_log(values[i - 1][1]))

    if len(log_returns) < 24:
        return None

    lr_mean = sum(log_returns) / len(log_returns)
    lr_std = (sum((x - lr_mean) ** 2 for x in log_returns) / len(log_returns)) ** 0.5
    if lr_std < 1e-6:
        lr_std = 0.001

    # Projected log(CPI) for target month
    proj_log_mean = log_latest + months_ahead * lr_mean
    proj_log_std = (months_ahead ** 0.5) * lr_std

    # YoY = (exp(proj_log) / prior_year_val - 1) * 100
    # = (exp(proj_log - log_prior) - 1) * 100
    # Let Z = proj_log - log_prior ~ N(proj_log_mean - log_prior, proj_log_std^2)
    z_mean = proj_log_mean - log_prior
    z_std = proj_log_std

    # We need P(round((exp(Z) - 1) * 100, 1) == target)
    # i.e., P(target - 0.05 <= (exp(Z) - 1) * 100 < target + 0.05)
    # i.e., P((target - 0.05)/100 + 1 <= exp(Z) < (target + 0.05)/100 + 1)
    # i.e., P(ln(1 + (target-0.05)/100) <= Z < ln(1 + (target+0.05)/100))

    lo = _safe_log(1 + (target - 0.05) / 100)
    hi = _safe_log(1 + (target + 0.05) / 100)

    prob = stats.norm.cdf(hi, loc=z_mean, scale=z_std) - stats.norm.cdf(lo, loc=z_mean, scale=z_std)
    return max(0.001, min(0.999, prob))


def _safe_log(x):
    import math
    return math.log(max(x, 1e-10))


def predict_cpi_core_mom(market: dict) -> Optional[float]:
    """
    Predict probability that core CPI MoM for a given month rounds to target.
    Uses CPILFESL data available before close_time.
    """
    target = parse_exactly_pct(market["yes_sub_title"])
    if target is None:
        return None

    close_time = market["close_time"]
    obs = get_fred_before("CPILFESL", close_time)
    if len(obs) < 24:
        return None

    values = [(o["date"], o["value"]) for o in obs]
    target_my = parse_target_month_year(market["title"])
    if target_my is None:
        return None
    target_month, target_year = target_my

    target_date_str = f"{target_year}-{target_month:02d}-01"
    prev_month = target_month - 1 if target_month > 1 else 12
    prev_year = target_year if target_month > 1 else target_year - 1
    prev_date_str = f"{prev_year}-{prev_month:02d}-01"

    # Check if we already have the target month
    target_val = None
    prev_val = None
    for d, v in values:
        if d == target_date_str:
            target_val = v
        if d == prev_date_str:
            prev_val = v

    if prev_val is None:
        return None

    if target_val is not None:
        mom = round((target_val / prev_val - 1) * 100, 1)
        return 1.0 if mom == target else 0.0

    # Project using historical MoM distribution
    latest_val = values[-1][1]
    latest_date = values[-1][0]
    latest_year = int(latest_date[:4])
    latest_month = int(latest_date[5:7])
    months_ahead = (target_year - latest_year) * 12 + (target_month - latest_month)

    if months_ahead < 0:
        return None

    # Compute historical MoM changes
    mom_changes = []
    for i in range(1, len(values)):
        p = values[i - 1][1]
        c = values[i][1]
        if p > 0:
            mom_changes.append((c / p - 1) * 100)

    if len(mom_changes) < 12:
        return None

    mom_mean = sum(mom_changes) / len(mom_changes)
    mom_std = (sum((x - mom_mean) ** 2 for x in mom_changes) / len(mom_changes)) ** 0.5
    if mom_std < 0.01:
        mom_std = 0.05

    if months_ahead <= 1:
        # Direct: the next MoM change follows our distribution
        # P(round(MoM, 1) == target) = P(target - 0.05 <= MoM < target + 0.05)
        lo = target - 0.05
        hi = target + 0.05
        prob = stats.norm.cdf(hi, loc=mom_mean, scale=mom_std) - stats.norm.cdf(lo, loc=mom_mean, scale=mom_std)
    else:
        # Multi-step: we need to project latest to prev_month, then compute
        # the MoM for the target month. This is more complex.
        # Simplify: the MoM for a given month is roughly drawn from the
        # same distribution regardless of intervening months.
        lo = target - 0.05
        hi = target + 0.05
        # Add uncertainty for extra months of projection
        effective_std = mom_std * (1 + 0.1 * (months_ahead - 1))
        prob = stats.norm.cdf(hi, loc=mom_mean, scale=effective_std) - stats.norm.cdf(lo, loc=mom_mean, scale=effective_std)

    return max(0.001, min(0.999, prob))


def predict_gas_above(market: dict, series_id: str = "GASREGW") -> Optional[float]:
    """
    Predict probability that gas price will be above the threshold.
    Uses EIA/FRED gas price data available before close_time.
    """
    threshold = parse_above_value(market["yes_sub_title"])
    if threshold is None:
        return None

    close_time = market["close_time"]
    obs = get_fred_before(series_id, close_time)
    if len(obs) < 10:
        return None

    # Use the most recent gas price as the baseline
    recent_prices = [o["value"] for o in obs[-52:]]  # last year of data
    latest = recent_prices[-1]

    # Compute weekly changes for volatility
    changes = []
    for i in range(1, len(recent_prices)):
        changes.append(recent_prices[i] - recent_prices[i - 1])

    if len(changes) < 4:
        return None

    change_mean = sum(changes) / len(changes)
    change_std = (sum((x - change_mean) ** 2 for x in changes) / len(changes)) ** 0.5
    if change_std < 0.001:
        change_std = 0.01

    # For weekly/daily gas, the market typically resolves at end of the period.
    # The question is whether the average gas price will be above threshold.
    # Simple model: assume next price ~ N(latest + drift, volatility)

    # How many periods ahead?
    latest_date = obs[-1]["date"]
    latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days_ahead = max(1, (close_time - latest_dt).days)

    if series_id == "GASREGW":
        periods_ahead = max(1, days_ahead / 7)
    else:
        periods_ahead = max(1, days_ahead)

    # Projected price distribution
    proj_mean = latest + periods_ahead * change_mean
    proj_std = (periods_ahead ** 0.5) * change_std

    # P(price > threshold)
    prob = 1.0 - stats.norm.cdf(threshold, loc=proj_mean, scale=proj_std)
    return max(0.001, min(0.999, prob))


def predict_core_cpi_annual(market: dict) -> Optional[float]:
    """
    Predict probability for annual core CPI range buckets.
    Uses CPILFESL from FRED.
    """
    rng = parse_range_pct(market["yes_sub_title"])
    if rng is None:
        return None
    lo_pct, hi_pct = rng

    close_time = market["close_time"]
    obs = get_fred_before("CPILFESL", close_time)
    if len(obs) < 24:
        return None

    target_year = parse_target_year(market["title"])
    if target_year is None:
        return None

    values = [(o["date"], o["value"]) for o in obs]

    # Find Dec of target_year and Dec of target_year - 1
    dec_target = None
    dec_prior = None
    for d, v in values:
        if d == f"{target_year}-12-01":
            dec_target = v
        if d == f"{target_year - 1}-12-01":
            dec_prior = v

    if dec_prior is None:
        return None

    if dec_target is not None:
        # We have actual data
        annual = (dec_target / dec_prior - 1) * 100
        in_range = (lo_pct <= annual <= hi_pct) if hi_pct != float("inf") else (annual >= lo_pct)
        if lo_pct == float("-inf"):
            in_range = annual <= hi_pct
        return 1.0 if in_range else 0.0

    # Project using trailing YoY
    latest_val = values[-1][1]
    latest_date = values[-1][0]

    # Compute historical annual changes
    annual_changes = []
    for i in range(12, len(values)):
        p = values[i - 12][1]
        c = values[i][1]
        if p > 0:
            annual_changes.append((c / p - 1) * 100)

    if len(annual_changes) < 12:
        return None

    # Use recent trend
    recent_annual = annual_changes[-12:]
    ann_mean = sum(recent_annual) / len(recent_annual)
    ann_std = (sum((x - ann_mean) ** 2 for x in recent_annual) / len(recent_annual)) ** 0.5
    if ann_std < 0.1:
        ann_std = 0.3

    # Project: how many more months until Dec of target_year?
    latest_y = int(latest_date[:4])
    latest_m = int(latest_date[5:7])
    months_to_dec = (target_year - latest_y) * 12 + (12 - latest_m)

    # Add uncertainty for projection
    effective_std = ann_std * (1 + 0.05 * months_to_dec)

    if hi_pct == float("inf"):
        prob = 1.0 - stats.norm.cdf(lo_pct, loc=ann_mean, scale=effective_std)
    elif lo_pct == float("-inf"):
        prob = stats.norm.cdf(hi_pct, loc=ann_mean, scale=effective_std)
    else:
        prob = (stats.norm.cdf(hi_pct, loc=ann_mean, scale=effective_std) -
                stats.norm.cdf(lo_pct, loc=ann_mean, scale=effective_std))

    return max(0.001, min(0.999, prob))


def predict_fed_rate(market: dict) -> Optional[float]:
    """
    Predict probability that fed funds rate upper bound will be above threshold.
    Uses DFEDTARU from FRED. The Fed rarely surprises, so the most recent rate
    plus any announced changes is a strong predictor.
    """
    threshold = parse_above_value(market["yes_sub_title"])
    if threshold is None:
        return None

    close_time = market["close_time"]
    obs = get_fred_before("DFEDTARU", close_time)
    if len(obs) < 5:
        return None

    # The current rate is the strongest predictor
    current_rate = obs[-1]["value"]

    # Historical volatility of rate changes between FOMC meetings (~6 weeks)
    # The fed moves in 0.25% increments, and changes are rare
    changes = []
    for i in range(1, len(obs)):
        c = obs[i]["value"] - obs[i - 1]["value"]
        if abs(c) > 0.001:
            changes.append(c)

    # For Fed funds, the rate almost never surprises at the meeting itself.
    # The market prices already reflect expectations well.
    # We use a simple model: current rate with small uncertainty.

    # Standard deviation of rate changes (daily data, so many zeros)
    # Use a wider window for meaningful std
    recent_rates = [o["value"] for o in obs[-90:]]  # ~3 months
    rate_changes = [recent_rates[i] - recent_rates[i - 1] for i in range(1, len(recent_rates))]
    rate_std = (sum(x ** 2 for x in rate_changes) / max(1, len(rate_changes))) ** 0.5

    # If rate hasn't moved recently, use a small uncertainty
    if rate_std < 0.01:
        rate_std = 0.05  # 5bp uncertainty

    # Days until resolution
    latest_date = obs[-1]["date"]
    latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days_ahead = max(1, (close_time - latest_dt).days)

    # Scale uncertainty with time
    effective_std = rate_std * (days_ahead ** 0.5)
    # Cap uncertainty -- fed doesn't move that much
    effective_std = min(effective_std, 0.50)

    prob = 1.0 - stats.norm.cdf(threshold, loc=current_rate, scale=effective_std)
    return max(0.001, min(0.999, prob))


# ──────────────────────────────────────────────────────────────────────
# Dispatch prediction to the right model
# ──────────────────────────────────────────────────────────────────────

def predict(market: dict) -> Optional[float]:
    """Route a market to the appropriate prediction function."""
    series = market["series_ticker"]
    try:
        if series == "KXECONSTATCPIYOY":
            return predict_cpi_yoy(market)
        elif series == "KXECONSTATCPICORE":
            return predict_cpi_core_mom(market)
        elif series in ("KXAAAGASW", "KXAAAGASM"):
            return predict_gas_above(market, "GASREGW")
        elif series == "KXAAAGASD":
            return predict_gas_above(market, "GASREGW")
        elif series == "KXCPICOREA":
            return predict_core_cpi_annual(market)
        elif series == "KXFED":
            return predict_fed_rate(market)
    except Exception as e:
        log.warning(f"Prediction error for {market['market_ticker']}: {e}")
        return None
    return None


# ──────────────────────────────────────────────────────────────────────
# Backtest scoring
# ──────────────────────────────────────────────────────────────────────

def brier_score(prob: float, outcome: int) -> float:
    """Brier score: (prob - outcome)^2.  Lower is better."""
    return (prob - outcome) ** 2


def simulate_bet(our_prob: float, market_price: float, outcome: int,
                 edge_threshold: float = 0.10, bet_size: float = 100.0) -> Optional[float]:
    """
    Simulate a $bet_size bet when |our_prob - market_price| > edge_threshold.
    Returns P&L or None if no bet.

    If our_prob > market_price + threshold: buy YES at market_price
        Win: +bet_size * (1 - market_price) / market_price  (profit on cost basis)
        Lose: -bet_size
    If our_prob < market_price - threshold: buy NO at (1 - market_price)
        Win (outcome=no): +bet_size * market_price / (1 - market_price)
        Lose: -bet_size

    Actually, on Kalshi:
    - Buy YES at price p: pay p, receive 1 if yes, 0 if no
    - Buy NO at price (1-p): pay (1-p), receive 1 if no, 0 if yes

    For $100 position:
    - Buy YES: buy 100/p contracts at price p each, cost = $100
      Win: receive 100/p * $1 = $100/p, profit = 100/p - 100 = 100*(1-p)/p
      Lose: receive 0, loss = -$100
    - Buy NO: buy 100/(1-p) contracts at price (1-p) each, cost = $100
      Win (no): receive 100/(1-p), profit = 100*p/(1-p)
      Lose (yes): loss = -$100
    """
    edge = our_prob - market_price

    if abs(edge) < edge_threshold:
        return None  # No bet

    if edge > 0:
        # We think YES is underpriced, buy YES
        if outcome == 1:
            return bet_size * (1 - market_price) / market_price
        else:
            return -bet_size
    else:
        # We think NO is underpriced, buy NO
        if outcome == 0:
            return bet_size * market_price / (1 - market_price)
        else:
            return -bet_size


def _save_results_to_db(markets: list[dict], results_by_series: dict[str, list[dict]]):
    """Persist all scored backtest results to kalshi.backtest_results."""
    model_version = "v2_naive_earlyprice"
    rows = []
    for series, res_list in results_by_series.items():
        for r in res_list:
            # Find the original market to get series_ticker
            rows.append((
                r["ticker"],
                series,
                r["title"],
                r["sub_title"],
                r["close_time"],
                r["result"],
                float(r["market_price"]),
                float(r["our_prob"]),
                float(r["edge"]),
                float(r["our_brier"]),
                float(r["mkt_brier"]),
                float(r["pnl"]) if r["pnl"] is not None else None,
                r["pnl"] is not None,
                model_version,
            ))

    if not rows:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO kalshi.backtest_results
                    (market_ticker, series_ticker, market_title, yes_sub_title,
                     close_time, result, market_price, our_estimate, edge,
                     our_brier, mkt_brier, pnl, bet_taken, model_version,
                     backtested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (market_ticker, model_version) DO UPDATE SET
                    our_estimate = EXCLUDED.our_estimate,
                    edge = EXCLUDED.edge,
                    our_brier = EXCLUDED.our_brier,
                    mkt_brier = EXCLUDED.mkt_brier,
                    pnl = EXCLUDED.pnl,
                    bet_taken = EXCLUDED.bet_taken,
                    backtested_at = NOW()
            """, rows)
        conn.commit()
        log.info(f"Saved {len(rows)} backtest results to DB")
        console.print(f"[green]Saved {len(rows)} results to kalshi.backtest_results[/green]\n")
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to save backtest results: {e}")
        console.print(f"[red]Failed to save results to DB: {e}[/red]\n")
    finally:
        conn.close()


def run_backtest(series_ticker: Optional[str] = None):
    """Run the full backtest and print results."""
    console.print("\n[bold]Loading resolved markets...[/bold]")
    markets = load_resolved_markets(series_ticker)
    console.print(f"Loaded {len(markets)} resolved markets\n")

    if not markets:
        console.print("[red]No resolved markets found.[/red]")
        return

    # Pre-fetch all needed FRED series
    console.print("[bold]Fetching FRED data (cached after first run)...[/bold]")
    needed_series = set()
    for m in markets:
        s = m["series_ticker"]
        if s in ("KXECONSTATCPIYOY",):
            needed_series.add("CPIAUCSL")
        elif s in ("KXECONSTATCPICORE", "KXCPICOREA"):
            needed_series.add("CPILFESL")
        elif s in ("KXAAAGASW", "KXAAAGASM", "KXAAAGASD"):
            needed_series.add("GASREGW")
        elif s == "KXFED":
            needed_series.add("DFEDTARU")

    for sid in needed_series:
        fetch_fred_series(sid)
    console.print(f"FRED data ready ({', '.join(sorted(needed_series))})\n")

    # Run predictions
    results_by_series: dict[str, list[dict]] = {}
    skipped = 0

    for m in markets:
        prob = predict(m)
        if prob is None:
            skipped += 1
            continue

        outcome = 1 if m["result"] == "yes" else 0
        market_price = float(m["early_price"])

        if market_price <= 0 or market_price >= 1:
            # Can't compute meaningful scores for edge-case prices
            market_price = max(0.01, min(0.99, market_price))

        our_brier = brier_score(prob, outcome)
        mkt_brier = brier_score(market_price, outcome)
        pnl = simulate_bet(prob, market_price, outcome)

        result = {
            "ticker": m["market_ticker"],
            "title": m["title"],
            "sub_title": m["yes_sub_title"],
            "close_time": m["close_time"],
            "result": m["result"],
            "outcome": outcome,
            "market_price": market_price,
            "our_prob": prob,
            "edge": prob - market_price,
            "our_brier": our_brier,
            "mkt_brier": mkt_brier,
            "pnl": pnl,
        }

        series = m["series_ticker"]
        results_by_series.setdefault(series, []).append(result)

    # ── Save results to DB ──
    _save_results_to_db(markets, results_by_series)

    # ── Print per-series summary ──
    summary_table = Table(title="Backtest Summary by Series", show_lines=True)
    summary_table.add_column("Series", style="cyan")
    summary_table.add_column("Markets", justify="right")
    summary_table.add_column("Our Brier", justify="right")
    summary_table.add_column("Mkt Brier", justify="right")
    summary_table.add_column("Brier Edge", justify="right")
    summary_table.add_column("Bets", justify="right")
    summary_table.add_column("Wins", justify="right")
    summary_table.add_column("Win Rate", justify="right")
    summary_table.add_column("Total P&L", justify="right")
    summary_table.add_column("Avg Edge", justify="right")

    total_pnl = 0.0
    total_bets = 0
    total_wins = 0
    all_our_brier = []
    all_mkt_brier = []

    for series in SUPPORTED_SERIES:
        if series not in results_by_series:
            continue
        res = results_by_series[series]
        n = len(res)
        avg_our = sum(r["our_brier"] for r in res) / n
        avg_mkt = sum(r["mkt_brier"] for r in res) / n
        brier_edge = avg_mkt - avg_our  # positive means we're better

        bets = [r for r in res if r["pnl"] is not None]
        n_bets = len(bets)
        wins = sum(1 for b in bets if b["pnl"] > 0)
        pnl_sum = sum(b["pnl"] for b in bets)
        avg_edge = sum(abs(b["edge"]) for b in bets) / n_bets if n_bets > 0 else 0

        total_pnl += pnl_sum
        total_bets += n_bets
        total_wins += wins
        all_our_brier.extend(r["our_brier"] for r in res)
        all_mkt_brier.extend(r["mkt_brier"] for r in res)

        pnl_style = "green" if pnl_sum >= 0 else "red"
        brier_style = "green" if brier_edge > 0 else "red"

        summary_table.add_row(
            series,
            str(n),
            f"{avg_our:.4f}",
            f"{avg_mkt:.4f}",
            f"[{brier_style}]{brier_edge:+.4f}[/{brier_style}]",
            str(n_bets),
            str(wins),
            f"{wins / n_bets * 100:.0f}%" if n_bets > 0 else "N/A",
            f"[{pnl_style}]${pnl_sum:+,.0f}[/{pnl_style}]",
            f"{avg_edge:.2%}" if n_bets > 0 else "N/A",
        )

    console.print(summary_table)

    # ── Overall summary ──
    if all_our_brier:
        console.print(f"\n[bold]Overall:[/bold]")
        overall_our = sum(all_our_brier) / len(all_our_brier)
        overall_mkt = sum(all_mkt_brier) / len(all_mkt_brier)
        console.print(f"  Markets scored: {len(all_our_brier)}  (skipped: {skipped})")
        console.print(f"  Our avg Brier:  {overall_our:.4f}")
        console.print(f"  Mkt avg Brier:  {overall_mkt:.4f}")
        edge_color = "green" if overall_mkt > overall_our else "red"
        console.print(f"  Brier edge:     [{edge_color}]{overall_mkt - overall_our:+.4f}[/{edge_color}]")
        console.print(f"  Total bets:     {total_bets}")
        if total_bets > 0:
            console.print(f"  Win rate:       {total_wins / total_bets * 100:.1f}%")
            pnl_color = "green" if total_pnl >= 0 else "red"
            console.print(f"  Total P&L:      [{pnl_color}]${total_pnl:+,.0f}[/{pnl_color}]")

    # ── Detailed per-market table for bets taken ──
    bet_table = Table(title="\nDetailed Bets (|edge| > 10%)", show_lines=False)
    bet_table.add_column("Ticker", style="dim", max_width=35)
    bet_table.add_column("Sub-title", max_width=20)
    bet_table.add_column("Close", max_width=12)
    bet_table.add_column("Result")
    bet_table.add_column("Mkt", justify="right")
    bet_table.add_column("Ours", justify="right")
    bet_table.add_column("Edge", justify="right")
    bet_table.add_column("P&L", justify="right")

    all_bets = []
    for res_list in results_by_series.values():
        for r in res_list:
            if r["pnl"] is not None:
                all_bets.append(r)

    all_bets.sort(key=lambda x: x["close_time"])

    for b in all_bets[:80]:  # limit display
        pnl_style = "green" if b["pnl"] > 0 else "red"
        edge_style = "green" if (b["edge"] > 0 and b["outcome"] == 1) or (b["edge"] < 0 and b["outcome"] == 0) else "red"
        bet_table.add_row(
            b["ticker"],
            b["sub_title"],
            b["close_time"].strftime("%Y-%m-%d"),
            b["result"],
            f"{b['market_price']:.2f}",
            f"{b['our_prob']:.2f}",
            f"[{edge_style}]{b['edge']:+.2f}[/{edge_style}]",
            f"[{pnl_style}]${b['pnl']:+.0f}[/{pnl_style}]",
        )

    if len(all_bets) > 80:
        bet_table.add_row("...", f"({len(all_bets) - 80} more)", "", "", "", "", "", "")

    console.print(bet_table)
    console.print()
