"""
Prophet-based backtester for time-series Kalshi markets.

Reconstructs the underlying time series (temperature, FX rate, CPI) from
resolved market buckets, fits Prophet on historical data (no lookahead),
and estimates probabilities for each bucket.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from prophet import Prophet
from psycopg2.extras import execute_batch
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
from scipy import stats

from trawler.db.connection import get_connection

# Suppress Prophet/cmdstanpy verbose logging
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

log = logging.getLogger(__name__)
console = Console()

MODEL_VERSION = "v2_prophet_earlyprice"
MIN_HISTORY_DAYS = 60
REFIT_EVERY_DAYS = 7  # refit Prophet weekly to keep speed reasonable

# Series configuration
WEATHER_SERIES = [
    "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA",
    "KXHIGHAUS", "KXHIGHDEN", "KXHIGHLAX",
]
ECON_SERIES = ["KXCPIYOY", "KXCPICOREYOY"]
FX_SERIES = ["KXEURUSD"]

ALL_SERIES = WEATHER_SERIES + ECON_SERIES + FX_SERIES


def _parse_weather_value(sub_title: str) -> Optional[float]:
    """Parse temperature from yes_sub_title. Returns midpoint or bound."""
    if not sub_title:
        return None
    # "54° to 55°" -> midpoint
    m = re.match(r"(\d+)°?\s+to\s+(\d+)°?", sub_title)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    # "58° or above" -> lower bound
    m = re.match(r"(\d+)°?\s+or\s+above", sub_title)
    if m:
        return float(m.group(1))
    # "49° or below" -> upper bound
    m = re.match(r"(\d+)°?\s+or\s+below", sub_title)
    if m:
        return float(m.group(1))
    return None


def _parse_fx_value(sub_title: str) -> Optional[float]:
    """Parse FX rate from yes_sub_title."""
    if not sub_title:
        return None
    # "1.15200 to 1.15399" -> midpoint
    m = re.match(r"([\d.]+)\s+to\s+([\d.]+)", sub_title)
    if m:
        return (float(m.group(1)) + float(m.group(2))) / 2.0
    # "1.16600 or above"
    m = re.match(r"([\d.]+)\s+or\s+above", sub_title)
    if m:
        return float(m.group(1))
    # "1.15000 or below"
    m = re.match(r"([\d.]+)\s+or\s+below", sub_title)
    if m:
        return float(m.group(1))
    return None


def _parse_cpi_value(sub_title: str) -> Optional[float]:
    """Parse CPI percentage from yes_sub_title like 'Above 2.6%'."""
    if not sub_title:
        return None
    m = re.match(r"Above\s+([\d.]+)%", sub_title)
    if m:
        return float(m.group(1))
    return None


def _get_parser(series_ticker: str):
    """Return the appropriate parser for a series."""
    if series_ticker in WEATHER_SERIES:
        return _parse_weather_value
    elif series_ticker in FX_SERIES:
        return _parse_fx_value
    elif series_ticker in ECON_SERIES:
        return _parse_cpi_value
    return None


def _parse_bucket_bounds(sub_title: str, series_ticker: str) -> Optional[tuple]:
    """Parse bucket lower and upper bounds from sub_title.

    Returns (lower, upper) where None means unbounded.
    """
    if not sub_title:
        return None

    if series_ticker in WEATHER_SERIES:
        m = re.match(r"(\d+)°?\s+to\s+(\d+)°?", sub_title)
        if m:
            return (float(m.group(1)), float(m.group(2)))
        m = re.match(r"(\d+)°?\s+or\s+above", sub_title)
        if m:
            return (float(m.group(1)), None)
        m = re.match(r"(\d+)°?\s+or\s+below", sub_title)
        if m:
            return (None, float(m.group(1)))

    elif series_ticker in FX_SERIES:
        m = re.match(r"([\d.]+)\s+to\s+([\d.]+)", sub_title)
        if m:
            return (float(m.group(1)), float(m.group(2)))
        m = re.match(r"([\d.]+)\s+or\s+above", sub_title)
        if m:
            return (float(m.group(1)), None)
        m = re.match(r"([\d.]+)\s+or\s+below", sub_title)
        if m:
            return (None, float(m.group(1)))

    elif series_ticker in ECON_SERIES:
        m = re.match(r"Above\s+([\d.]+)%", sub_title)
        if m:
            val = float(m.group(1))
            return (val, None)  # "Above X%" is unbounded above

    return None


def _reconstruct_time_series(
    markets: list[dict],
    series_ticker: str,
    before_time=None,
) -> pd.DataFrame:
    """Reconstruct the underlying time series from resolved 'yes' markets.

    For CPI: find the highest 'Above X%' that resolved yes per event date.
    For weather/FX: the single 'yes' bucket gives the value.

    Returns DataFrame with 'ds' and 'y' columns.
    """
    parser = _get_parser(series_ticker)
    if parser is None:
        return pd.DataFrame(columns=["ds", "y"])

    if series_ticker in ECON_SERIES:
        # Group by close_time, find highest "Above X%" that resolved yes
        from collections import defaultdict
        date_values = defaultdict(list)
        for m in markets:
            if before_time and m["close_time"] >= before_time:
                continue
            if m["result"] != "yes":
                continue
            val = parser(m["yes_sub_title"])
            if val is not None:
                dt = m["close_time"].date() if hasattr(m["close_time"], "date") else m["close_time"]
                date_values[dt].append(val)

        rows = []
        for dt, vals in sorted(date_values.items()):
            # The actual CPI is approximately the highest "Above X%" that resolved yes
            rows.append({"ds": pd.Timestamp(dt), "y": max(vals)})
    else:
        # Weather and FX: one "yes" bucket per event
        seen_dates = {}
        for m in markets:
            if before_time and m["close_time"] >= before_time:
                continue
            if m["result"] != "yes":
                continue
            val = parser(m["yes_sub_title"])
            if val is not None:
                dt = m["close_time"].date() if hasattr(m["close_time"], "date") else m["close_time"]
                seen_dates[dt] = val

        rows = [{"ds": pd.Timestamp(dt), "y": v} for dt, v in sorted(seen_dates.items())]

    return pd.DataFrame(rows)


def _bucket_probability(
    forecast_row: pd.Series,
    bounds: tuple,
    series_ticker: str,
) -> float:
    """Estimate P(value falls in bucket) using Prophet forecast distribution.

    Assumes approximately normal distribution with mean=yhat and
    std derived from the prediction interval.
    """
    yhat = forecast_row["yhat"]
    # Prophet's default interval is 80%. yhat_upper - yhat_lower spans ~2.56 std devs
    interval_width = forecast_row["yhat_upper"] - forecast_row["yhat_lower"]
    std = max(interval_width / 2.56, 1e-6)  # 80% interval = 1.28 std on each side

    lower, upper = bounds

    if series_ticker in ECON_SERIES:
        # "Above X%" -> P(value > X)
        if lower is not None:
            return float(1.0 - stats.norm.cdf(lower, loc=yhat, scale=std))
        return 0.5

    # Range bucket
    if lower is not None and upper is not None:
        p = stats.norm.cdf(upper, loc=yhat, scale=std) - stats.norm.cdf(lower, loc=yhat, scale=std)
    elif lower is not None:
        # "X or above"
        p = 1.0 - stats.norm.cdf(lower, loc=yhat, scale=std)
    elif upper is not None:
        # "X or below"
        p = stats.norm.cdf(upper, loc=yhat, scale=std)
    else:
        p = 0.5

    return float(max(0.001, min(0.999, p)))


def _fit_prophet(df: pd.DataFrame, series_ticker: str) -> Prophet:
    """Fit a Prophet model on the time series."""
    if series_ticker in WEATHER_SERIES:
        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
        )
    elif series_ticker in ECON_SERIES:
        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
    else:  # FX
        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
        )

    m.fit(df)
    return m


def _load_all_markets(series_ticker: str) -> list[dict]:
    """Load all resolved markets for a series, ordered by close_time."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT market_ticker, series_ticker, title, yes_sub_title,
                  result, early_price, close_time
           FROM kalshi.historical_resolutions
           WHERE series_ticker = %s
             AND result IS NOT NULL
             AND early_price IS NOT NULL
           ORDER BY close_time, yes_sub_title""",
        (series_ticker,),
    )
    cols = [d[0] for d in cur.description]
    markets = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    return markets


def prophet_predict(series_ticker: str) -> list[dict]:
    """Run Prophet-based prediction for all resolved markets in a series.

    No lookahead: for each target date, only uses data from before that date.
    Refits Prophet every REFIT_EVERY_DAYS days for performance.
    """
    markets = _load_all_markets(series_ticker)
    if not markets:
        return []

    # Separate "yes" markets for time series reconstruction
    yes_markets = [m for m in markets if m["result"] == "yes"]

    # Group all markets by date (close_time date)
    from collections import defaultdict
    markets_by_date = defaultdict(list)
    for m in markets:
        dt = m["close_time"].date() if hasattr(m["close_time"], "date") else m["close_time"]
        markets_by_date[dt].append(m)

    sorted_dates = sorted(markets_by_date.keys())

    results = []
    cached_model = None
    cached_model_date = None
    last_fit_date_idx = -1

    for date_idx, target_date in enumerate(sorted_dates):
        if date_idx < MIN_HISTORY_DAYS:
            continue

        target_dt = datetime.combine(target_date, datetime.min.time()).replace(
            tzinfo=timezone.utc
        )

        # Build time series from all data BEFORE this date
        ts_df = _reconstruct_time_series(yes_markets, series_ticker, before_time=target_dt)

        if len(ts_df) < MIN_HISTORY_DAYS:
            continue

        # Refit model if needed (every REFIT_EVERY_DAYS or if no model yet)
        need_refit = (
            cached_model is None
            or (date_idx - last_fit_date_idx) >= REFIT_EVERY_DAYS
        )

        if need_refit:
            try:
                cached_model = _fit_prophet(ts_df, series_ticker)
                cached_model_date = target_date
                last_fit_date_idx = date_idx
            except Exception as e:
                log.warning(f"Prophet fit failed for {series_ticker} at {target_date}: {e}")
                continue

        # Forecast for target date
        future = pd.DataFrame({"ds": [pd.Timestamp(target_date)]})
        try:
            forecast = cached_model.predict(future)
        except Exception as e:
            log.warning(f"Prophet predict failed for {series_ticker} at {target_date}: {e}")
            continue

        forecast_row = forecast.iloc[0]

        # Score each market bucket for this date
        for market in markets_by_date[target_date]:
            bounds = _parse_bucket_bounds(market["yes_sub_title"], series_ticker)
            if bounds is None:
                continue

            our_estimate = _bucket_probability(forecast_row, bounds, series_ticker)
            market_price = float(market["early_price"])
            market_price = max(0.01, min(0.99, market_price))
            outcome = 1 if market["result"] == "yes" else 0
            edge = our_estimate - market_price

            our_brier = (our_estimate - outcome) ** 2
            mkt_brier = (market_price - outcome) ** 2

            # Simulate bet — skip markets at extreme settlement prices
            # (>0.95 or <0.05) since those represent post-settlement and
            # there's no real trading opportunity.
            pnl = None
            if abs(edge) > 0.10 and 0.05 <= market_price <= 0.95:
                if edge > 0:
                    # Buy YES
                    pnl = 100.0 * (1 - market_price) / market_price if outcome == 1 else -100.0
                else:
                    # Buy NO
                    pnl = 100.0 * market_price / (1 - market_price) if outcome == 0 else -100.0

            results.append({
                "market_ticker": market["market_ticker"],
                "series_ticker": series_ticker,
                "title": market["title"],
                "yes_sub_title": market["yes_sub_title"],
                "close_time": market["close_time"],
                "result": market["result"],
                "outcome": outcome,
                "market_price": market_price,
                "our_estimate": our_estimate,
                "edge": edge,
                "our_brier": our_brier,
                "mkt_brier": mkt_brier,
                "pnl": pnl,
                "bet_taken": pnl is not None,
            })

    return results


def _save_results(all_results: list[dict]):
    """Save Prophet backtest results to kalshi.backtest_results."""
    if not all_results:
        return

    rows = []
    for r in all_results:
        rows.append((
            r["market_ticker"],
            r["series_ticker"],
            r["title"],
            r["yes_sub_title"],
            r["close_time"],
            r["result"],
            r["market_price"],
            r["our_estimate"],
            r["edge"],
            r["our_brier"],
            r["mkt_brier"],
            r["pnl"],
            r["bet_taken"],
            MODEL_VERSION,
        ))

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
            """, rows, page_size=500)
        conn.commit()
        log.info(f"Saved {len(rows)} Prophet backtest results to DB")
    except Exception as e:
        conn.rollback()
        log.error(f"Failed to save Prophet results: {e}")
        console.print(f"[red]Failed to save results to DB: {e}[/red]")
        raise
    finally:
        conn.close()


def _print_summary(series_summaries: list[dict]):
    """Print Rich table with backtest results."""
    summary_table = Table(
        title=f"Prophet Backtest -- {len(series_summaries)} Series (sorted by P&L)",
        show_lines=False,
    )
    summary_table.add_column("Series", style="cyan", max_width=25)
    summary_table.add_column("Scored", justify="right")
    summary_table.add_column("Our Brier", justify="right")
    summary_table.add_column("Mkt Brier", justify="right")
    summary_table.add_column("B.Edge", justify="right")
    summary_table.add_column("Bets", justify="right")
    summary_table.add_column("Wins", justify="right")
    summary_table.add_column("Win%", justify="right")
    summary_table.add_column("P&L", justify="right")

    grand_pnl = 0.0
    grand_bets = 0
    grand_wins = 0
    grand_scored = 0

    for s in series_summaries:
        pnl_style = "green" if s["total_pnl"] >= 0 else "red"
        brier_style = "green" if s["brier_edge"] > 0 else "red"
        win_rate = f"{s['wins'] / s['n_bets'] * 100:.0f}%" if s["n_bets"] > 0 else "-"

        summary_table.add_row(
            s["series"],
            str(s["scored"]),
            f"{s['avg_our_brier']:.4f}",
            f"{s['avg_mkt_brier']:.4f}",
            f"[{brier_style}]{s['brier_edge']:+.4f}[/{brier_style}]",
            str(s["n_bets"]),
            str(s["wins"]),
            win_rate,
            f"[{pnl_style}]${s['total_pnl']:+,.0f}[/{pnl_style}]",
        )

        grand_pnl += s["total_pnl"]
        grand_bets += s["n_bets"]
        grand_wins += s["wins"]
        grand_scored += s["scored"]

    console.print(summary_table)

    console.print(f"\n[bold]Overall Summary[/bold]")
    console.print(f"  Series backtested: {len(series_summaries)}")
    console.print(f"  Markets scored:    {grand_scored:,}")
    console.print(f"  Total bets:        {grand_bets:,}")
    if grand_bets > 0:
        console.print(f"  Win rate:          {grand_wins / grand_bets * 100:.1f}%")
        pnl_color = "green" if grand_pnl >= 0 else "red"
        console.print(f"  Total P&L:         [{pnl_color}]${grand_pnl:+,.0f}[/{pnl_color}]")
    console.print()


def run_prophet_backtest(series_ticker: Optional[str] = None):
    """Run Prophet-based backtest on time-series markets.

    If series_ticker is provided, run on just that series.
    Otherwise, run on all supported time-series.
    """
    if series_ticker:
        target_series = [series_ticker]
    else:
        target_series = WEATHER_SERIES  # default to weather (most data)

    console.print(f"\n[bold]Prophet Backtest[/bold]")
    console.print(f"Series: {', '.join(target_series)}")
    console.print(f"Min history: {MIN_HISTORY_DAYS} days, Refit every: {REFIT_EVERY_DAYS} days\n")

    all_results = []
    series_summaries = []

    with Progress() as progress:
        task = progress.add_task("Running Prophet backtest...", total=len(target_series))

        for st in target_series:
            console.print(f"  Processing [cyan]{st}[/cyan]...")
            results = prophet_predict(st)
            all_results.extend(results)

            if results:
                bets = [r for r in results if r["pnl"] is not None]
                total_pnl = sum(r["pnl"] for r in bets)
                n_bets = len(bets)
                wins = sum(1 for r in bets if r["pnl"] > 0)
                avg_our_brier = sum(r["our_brier"] for r in results) / len(results)
                avg_mkt_brier = sum(r["mkt_brier"] for r in results) / len(results)

                series_summaries.append({
                    "series": st,
                    "scored": len(results),
                    "n_bets": n_bets,
                    "wins": wins,
                    "total_pnl": total_pnl,
                    "avg_our_brier": avg_our_brier,
                    "avg_mkt_brier": avg_mkt_brier,
                    "brier_edge": avg_mkt_brier - avg_our_brier,
                })

                console.print(
                    f"    {len(results)} scored, {n_bets} bets, "
                    f"P&L: ${total_pnl:+,.0f}"
                )
            else:
                console.print(f"    No results for {st}")

            progress.advance(task)

    # Save to DB
    if all_results:
        console.print(f"\nSaving {len(all_results):,} results to DB...")
        _save_results(all_results)
        console.print(
            f"[green]Saved {len(all_results):,} results "
            f"(model_version='{MODEL_VERSION}')[/green]\n"
        )

    # Print summary
    series_summaries.sort(key=lambda x: x["total_pnl"], reverse=True)
    _print_summary(series_summaries)
