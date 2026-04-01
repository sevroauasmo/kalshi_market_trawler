"""Edge scanner — identify structural advantages across candidate series.

Classifies each candidate series by three edge types:
  1. Speed Edge — we can observe the underlying data faster than settlement source
  2. Covariate Edge — free leading indicators predict outcome before settlement
  3. Interpretation Edge — settlement data is public but hard to parse/analyze
"""

import json
import logging
from dataclasses import dataclass, field

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from trawler.db.connection import get_connection

log = logging.getLogger(__name__)
console = Console()

# ---------------------------------------------------------------------------
# Edge source mappings
# ---------------------------------------------------------------------------

SPEED_EDGE_SOURCES = {
    "forecast.weather.gov": {
        "faster_source": "aviationweather.gov METAR",
        "api": "https://aviationweather.gov/api/data/metar?ids=KJFK&format=json",
        "lead_time": "6-12 hours (know temp before CLI report publishes)",
        "score": 9,
    },
    "weather.gov": {
        "faster_source": "aviationweather.gov METAR + HRRR model",
        "api": "https://aviationweather.gov/api/data/metar?ids=KJFK&format=json",
        "lead_time": "6-12 hours",
        "score": 8,
    },
    "eia.gov": {
        "faster_source": "GasBuddy API / AAA daily fuel gauge",
        "lead_time": "3-7 days (daily vs weekly)",
        "score": 7,
    },
    "accuweather": {
        "faster_source": "aviationweather.gov METAR (free, raw)",
        "api": "https://aviationweather.gov/api/data/metar?format=json",
        "lead_time": "Real-time vs forecast snapshot",
        "score": 7,
    },
    "coinmarketcap": {
        "faster_source": "Binance/Coinbase websocket feeds",
        "lead_time": "Seconds-level (exchange API leads aggregator)",
        "score": 6,
    },
    "coingecko": {
        "faster_source": "Binance/Coinbase websocket feeds",
        "lead_time": "Seconds-level",
        "score": 6,
    },
}

COVARIATE_SOURCES = {
    "bls.gov/cpi": {
        "covariate": "Cleveland Fed Inflation Nowcast",
        "url": "https://www.clevelandfed.org/indicators-and-data/inflation-nowcasting",
        "lead_time": "Daily updates, ~2 weeks before BLS release",
        "score": 8,
    },
    "bls.gov/news.release/empsit": {
        "covariate": "ADP Employment + Weekly Jobless Claims",
        "lead_time": "2-5 days before BLS NFP",
        "score": 7,
    },
    "bls.gov": {
        "covariate": "Cleveland Fed Nowcast / ADP / PMI surveys / Fed regional surveys",
        "url": "https://www.clevelandfed.org/indicators-and-data/inflation-nowcasting",
        "lead_time": "Days to weeks before official BLS release",
        "score": 7,
    },
    "netflix.com/tudum": {
        "covariate": "Google Trends + social media mentions",
        "lead_time": "Real-time vs weekly Netflix publication",
        "score": 6,
    },
    "netflix.com": {
        "covariate": "Google Trends + social media mentions",
        "lead_time": "Real-time vs weekly",
        "score": 6,
    },
    "treasury.gov": {
        "covariate": "FRED daily rates + futures market implied rates",
        "lead_time": "Continuous market data vs periodic publication",
        "score": 5,
    },
    "fred.stlouisfed.org": {
        "covariate": "Multiple leading indicator composites (LEI, PMI, regional Fed surveys)",
        "lead_time": "Varies by series; many have advance private-sector analogs",
        "score": 6,
    },
}

INTERPRETATION_INDICATORS = {
    "data.ny.gov": {
        "score": 8,
        "note": "Open data portal -- raw data available but requires custom analysis",
    },
    "data.gov": {
        "score": 7,
        "note": "Federal open data -- often complex/multi-table",
    },
    "census.gov": {
        "score": 7,
        "note": "Census data requires understanding methodology",
    },
    "fred.stlouisfed.org": {
        "score": 6,
        "note": "FRED data is accessible but component analysis adds edge",
    },
    "bls.gov": {
        "score": 6,
        "note": "BLS microdata and subcomponents require domain knowledge",
    },
    "sec.gov": {
        "score": 7,
        "note": "SEC filings are dense; EDGAR API parsing adds edge",
    },
    "data.cms.gov": {
        "score": 7,
        "note": "CMS healthcare data is complex multi-table",
    },
    "usda.gov": {
        "score": 6,
        "note": "USDA crop/food data requires methodology knowledge",
    },
}

# Category-level heuristics for covariate mapping
CATEGORY_COVARIATES = {
    "Economics": {
        "covariate": "Cleveland Fed Nowcast, ADP, PMI, Fed regional surveys",
        "score": 7,
    },
    "Climate and Weather": {
        "covariate": "GFS/HRRR/ECMWF model outputs (free), METAR station data",
        "score": 8,
    },
    "Energy": {
        "covariate": "GasBuddy real-time, AAA daily fuel gauge, oil futures",
        "score": 7,
    },
    "Transportation": {
        "covariate": "FlightAware data, TSA daily throughput, airline schedules",
        "score": 6,
    },
    "Financial": {
        "covariate": "Futures markets, options-implied distributions, FRED daily",
        "score": 5,
    },
    "Companies": {
        "covariate": "Google Trends, app store rankings, SimilarWeb, social sentiment",
        "score": 5,
    },
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class EdgeResult:
    ticker: str
    title: str
    category: str
    edge_type: str  # Speed / Covariate / Interpretation
    edge_score: int  # 1-10
    data_source: str  # what we'd pull
    actionable_note: str
    resolved_count: int = 0
    avg_volume: float = 0.0


@dataclass
class SeriesInfo:
    ticker: str
    title: str
    category: str
    frequency: str
    settlement_sources: list[dict] = field(default_factory=list)
    resolved_count: int = 0
    avg_volume: float = 0.0
    total_volume: float = 0.0


# ---------------------------------------------------------------------------
# Source URL extraction helpers
# ---------------------------------------------------------------------------


def _get_source_urls(settlement_sources) -> list[str]:
    """Extract all URLs from settlement_sources (handles JSON string or list)."""
    if not settlement_sources:
        return []
    if isinstance(settlement_sources, str):
        try:
            settlement_sources = json.loads(settlement_sources)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(settlement_sources, list):
        return [s.get("url", "") for s in settlement_sources if isinstance(s, dict)]
    return []


def _get_source_url_string(settlement_sources) -> str:
    """Concatenate all source URLs into one lowercase string for matching."""
    return " ".join(_get_source_urls(settlement_sources)).lower()


# ---------------------------------------------------------------------------
# Edge detection functions
# ---------------------------------------------------------------------------


def detect_speed_edge(info: SeriesInfo) -> EdgeResult | None:
    """Check if settlement source has a faster real-time alternative."""
    url_str = _get_source_url_string(info.settlement_sources)
    if not url_str:
        return None

    best_match = None
    best_score = 0

    for pattern, meta in SPEED_EDGE_SOURCES.items():
        if pattern.lower() in url_str:
            if meta["score"] > best_score:
                best_score = meta["score"]
                best_match = meta

    if best_match and best_score >= 5:
        return EdgeResult(
            ticker=info.ticker,
            title=info.title,
            category=info.category or "",
            edge_type="Speed",
            edge_score=best_match["score"],
            data_source=best_match["faster_source"],
            actionable_note=f"Lead time: {best_match.get('lead_time', 'unknown')}. "
            f"Build real-time poller for {best_match['faster_source']}.",
            resolved_count=info.resolved_count,
            avg_volume=info.avg_volume,
        )
    return None


def detect_covariate_edge(info: SeriesInfo) -> EdgeResult | None:
    """Check if known leading indicators exist for this settlement source."""
    url_str = _get_source_url_string(info.settlement_sources)

    best_match = None
    best_score = 0

    # Check URL-based covariate matches (more specific first)
    for pattern, meta in COVARIATE_SOURCES.items():
        if pattern.lower() in url_str:
            if meta["score"] > best_score:
                best_score = meta["score"]
                best_match = meta

    # Fallback: category-level covariate heuristic
    if not best_match and info.category in CATEGORY_COVARIATES:
        cat_meta = CATEGORY_COVARIATES[info.category]
        best_match = cat_meta
        best_score = cat_meta["score"]

    if best_match and best_score >= 5:
        covariate_name = best_match.get("covariate", "Unknown leading indicator")
        return EdgeResult(
            ticker=info.ticker,
            title=info.title,
            category=info.category or "",
            edge_type="Covariate",
            edge_score=best_score,
            data_source=covariate_name,
            actionable_note=f"Lead time: {best_match.get('lead_time', 'varies')}. "
            f"Build ingestion for {covariate_name}.",
            resolved_count=info.resolved_count,
            avg_volume=info.avg_volume,
        )
    return None


def detect_interpretation_edge(info: SeriesInfo) -> EdgeResult | None:
    """Check if settlement data is public but hard to interpret."""
    url_str = _get_source_url_string(info.settlement_sources)

    best_match = None
    best_score = 0

    for pattern, meta in INTERPRETATION_INDICATORS.items():
        if pattern.lower() in url_str:
            if meta["score"] > best_score:
                best_score = meta["score"]
                best_match = meta

    # Boost score if title suggests complex multi-step analysis
    complexity_keywords = [
        "change", "yoy", "year-over-year", "percentage", "rate",
        "index", "subcomponent", "seasonally adjusted", "annualized",
        "cumulative", "rolling", "average", "median",
    ]
    title_lower = (info.title or "").lower()
    complexity_hits = sum(1 for kw in complexity_keywords if kw in title_lower)
    complexity_bonus = min(complexity_hits, 3)  # cap at +3

    # Boost if low volume relative to data availability (fewer participants)
    volume_bonus = 0
    if info.avg_volume and 0 < info.avg_volume < 500:
        volume_bonus = 2
    elif info.avg_volume and 0 < info.avg_volume < 2000:
        volume_bonus = 1

    if best_match:
        final_score = min(best_match["score"] + complexity_bonus + volume_bonus, 10)
        return EdgeResult(
            ticker=info.ticker,
            title=info.title,
            category=info.category or "",
            edge_type="Interpretation",
            edge_score=final_score,
            data_source=f"Raw data at {list(INTERPRETATION_INDICATORS.keys())[0]} etc.",
            actionable_note=best_match["note"]
            + (f" (+{complexity_bonus} complexity, +{volume_bonus} low-vol bonus)"
               if complexity_bonus or volume_bonus else ""),
            resolved_count=info.resolved_count,
            avg_volume=info.avg_volume,
        )
    return None


# ---------------------------------------------------------------------------
# METAR test pull
# ---------------------------------------------------------------------------


def test_metar_pull(station_ids: list[str] | None = None):
    """Test-pull METAR data from aviationweather.gov for given stations."""
    if station_ids is None:
        station_ids = ["KJFK", "KLAX", "KORD"]

    console.print()
    console.print(
        Panel("[bold cyan]METAR Speed-Edge Validation[/bold cyan]", expand=False)
    )

    for station in station_ids:
        url = f"https://aviationweather.gov/api/data/metar?ids={station}&format=json&hours=3"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data:
                latest = data[0]
                temp_c = latest.get("temp")
                dewp_c = latest.get("dewp")
                obs_time = latest.get("reportTime", latest.get("obsTime", "unknown"))
                raw_text = latest.get("rawOb", "N/A")
                temp_f = round(temp_c * 9 / 5 + 32, 1) if temp_c is not None else "N/A"

                console.print(f"\n  [green]{station}[/green] -- latest observation:")
                console.print(f"    Time:     {obs_time}")
                console.print(f"    Temp:     {temp_c}C / {temp_f}F")
                console.print(f"    Dewpoint: {dewp_c}C")
                console.print(f"    Raw:      {raw_text}")
            else:
                console.print(f"\n  [yellow]{station}[/yellow] -- no data returned")
        except Exception as e:
            console.print(f"\n  [red]{station}[/red] -- error: {e}")


# ---------------------------------------------------------------------------
# Main scanner
# ---------------------------------------------------------------------------


def _load_candidate_series() -> list[SeriesInfo]:
    """Load all candidate series with resolution stats from DB."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.ticker,
                    s.title,
                    s.category,
                    s.frequency,
                    s.settlement_sources,
                    COALESCE(agg.resolved_count, 0) AS resolved_count,
                    COALESCE(agg.avg_volume, 0) AS avg_volume,
                    COALESCE(agg.total_vol, 0) AS total_volume
                FROM kalshi.series_catalog s
                LEFT JOIN (
                    SELECT
                        series_ticker,
                        COUNT(*) FILTER (WHERE result IS NOT NULL) AS resolved_count,
                        AVG(volume) AS avg_volume,
                        SUM(volume) AS total_vol
                    FROM kalshi.historical_resolutions
                    GROUP BY series_ticker
                ) agg ON agg.series_ticker = s.ticker
                WHERE s.candidate_status = 'candidate'
                ORDER BY s.ticker
            """)
            rows = cur.fetchall()

        series_list = []
        for ticker, title, category, frequency, sources, res_count, avg_vol, total_vol in rows:
            # Parse settlement_sources
            parsed_sources = sources
            if isinstance(sources, str):
                try:
                    parsed_sources = json.loads(sources)
                except (json.JSONDecodeError, TypeError):
                    parsed_sources = []

            series_list.append(
                SeriesInfo(
                    ticker=ticker,
                    title=title or "",
                    category=category or "",
                    frequency=frequency or "",
                    settlement_sources=parsed_sources if parsed_sources else [],
                    resolved_count=res_count,
                    avg_volume=float(avg_vol) if avg_vol else 0.0,
                    total_volume=float(total_vol) if total_vol else 0.0,
                )
            )
        return series_list
    finally:
        conn.close()


def scan_for_edges():
    """Scan all candidate series and classify by edge type.

    Prints a Rich-formatted prioritized table of opportunities.
    """
    console.print(
        Panel(
            "[bold]Edge Scanner[/bold] -- scanning candidate series for structural advantages",
            style="cyan",
        )
    )

    series_list = _load_candidate_series()
    console.print(f"\nLoaded [bold]{len(series_list)}[/bold] candidate series from DB.\n")

    if not series_list:
        console.print("[yellow]No candidate series found. Run 'trawler scan' and 'trawler filter' first.[/yellow]")
        return

    # Collect all edges
    all_edges: list[EdgeResult] = []

    for info in series_list:
        # Check all three edge types; a series can appear under multiple types
        speed = detect_speed_edge(info)
        if speed:
            all_edges.append(speed)

        covariate = detect_covariate_edge(info)
        if covariate:
            all_edges.append(covariate)

        interpretation = detect_interpretation_edge(info)
        if interpretation:
            all_edges.append(interpretation)

    if not all_edges:
        console.print("[yellow]No edges detected across candidate series.[/yellow]")
        return

    # Sort: by edge type grouping, then score descending
    type_order = {"Speed": 0, "Covariate": 1, "Interpretation": 2}
    all_edges.sort(key=lambda e: (type_order.get(e.edge_type, 9), -e.edge_score))

    # Print summary
    console.print(
        f"Found [bold green]{len(all_edges)}[/bold green] edge opportunities "
        f"across {len(set(e.ticker for e in all_edges))} series.\n"
    )

    # Build Rich table
    for edge_type in ["Speed", "Covariate", "Interpretation"]:
        edges_of_type = [e for e in all_edges if e.edge_type == edge_type]
        if not edges_of_type:
            continue

        type_colors = {"Speed": "red", "Covariate": "magenta", "Interpretation": "blue"}
        color = type_colors.get(edge_type, "white")

        table = Table(
            title=f"\n[bold {color}]{edge_type} Edge[/bold {color}] ({len(edges_of_type)} series)",
            show_lines=True,
            expand=True,
        )
        table.add_column("Ticker", style="bold", max_width=20)
        table.add_column("Title", max_width=40)
        table.add_column("Category", max_width=18)
        table.add_column("Score", justify="center", max_width=6)
        table.add_column("Data Source", max_width=35)
        table.add_column("Actionable Note", max_width=50)
        table.add_column("Resolved", justify="right", max_width=8)
        table.add_column("Avg Vol", justify="right", max_width=10)

        for e in edges_of_type:
            score_style = "bold green" if e.edge_score >= 8 else ("yellow" if e.edge_score >= 6 else "dim")
            title_truncated = (e.title[:37] + "...") if len(e.title) > 40 else e.title

            table.add_row(
                e.ticker,
                title_truncated,
                e.category,
                Text(str(e.edge_score), style=score_style),
                e.data_source,
                e.actionable_note,
                str(e.resolved_count),
                f"${e.avg_volume:,.0f}" if e.avg_volume else "$0",
            )

        console.print(table)

    # Summary stats
    console.print()
    speed_count = len([e for e in all_edges if e.edge_type == "Speed"])
    cov_count = len([e for e in all_edges if e.edge_type == "Covariate"])
    interp_count = len([e for e in all_edges if e.edge_type == "Interpretation"])
    console.print(f"[bold]Summary:[/bold]  Speed={speed_count}  Covariate={cov_count}  Interpretation={interp_count}")

    # Top recommendations
    top_edges = sorted(all_edges, key=lambda e: -e.edge_score)[:5]
    console.print("\n[bold underline]Top 5 Opportunities:[/bold underline]")
    for i, e in enumerate(top_edges, 1):
        console.print(
            f"  {i}. [bold]{e.ticker}[/bold] ({e.edge_type}, score={e.edge_score}) -- {e.data_source}"
        )

    # Test METAR pull for top speed-edge weather candidates
    weather_speed = [
        e for e in all_edges
        if e.edge_type == "Speed" and "METAR" in e.data_source
    ]
    if weather_speed:
        # Try to extract station IDs from tickers (e.g., KXHIGHNY -> KJFK)
        # Fallback to standard stations
        test_metar_pull(["KJFK", "KLAX", "KORD"])

    console.print()
