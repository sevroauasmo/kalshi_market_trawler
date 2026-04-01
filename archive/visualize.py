"""Year-over-year interactive subway ridership chart."""

import os
import sys
import webbrowser
from pathlib import Path

import plotly.graph_objects as go

from kalshi_mta.fetch_data import DATA_DIR, load

OUTPUT_FILE = DATA_DIR / "yoy_ridership.html"

COLORS = [
    "#999999",  # 2020 - grey (COVID year, less prominent)
    "#e6194b",  # 2021 - red
    "#f58231",  # 2022 - orange
    "#ffe119",  # 2023 - yellow
    "#3cb44b",  # 2024 - green
    "#4363d8",  # 2025 - blue
    "#911eb4",  # 2026 - purple
]

MONTH_DAY_STARTS = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def create_figure() -> go.Figure:
    df = load()

    # Rolling average on the full chronological series, then split by year
    df = df.sort_values("date")
    df["rolling_avg"] = df["ridership"].rolling(7, min_periods=1).mean()
    df["year"] = df["date"].dt.year
    df["day_of_year"] = df["date"].dt.dayofyear
    df["month_day"] = df["date"].dt.strftime("%b %d")

    fig = go.Figure()
    years = sorted(df["year"].unique())

    for i, year in enumerate(years):
        ydf = df[df["year"] == year].sort_values("day_of_year")
        color = COLORS[i % len(COLORS)]
        peak = ydf.loc[ydf["rolling_avg"].idxmax()]
        peak_val = peak["rolling_avg"]
        peak_label = f"{year} (peak: {peak_val:,.0f})"

        fig.add_trace(go.Scatter(
            x=ydf["day_of_year"],
            y=ydf["rolling_avg"],
            mode="lines",
            name=peak_label,
            line=dict(color=color, width=2.5),
            customdata=ydf["month_day"],
            hovertemplate=(
                f"<b>{year}</b> — %{{customdata}}<br>"
                "7-Day Avg: %{y:,.0f}<extra></extra>"
            ),
        ))

        # Mark the yearly peak
        fig.add_trace(go.Scatter(
            x=[peak["day_of_year"]],
            y=[peak_val],
            mode="markers+text",
            marker=dict(color=color, size=10, symbol="star"),
            text=[f"{peak_val:,.0f}"],
            textposition="top center",
            textfont=dict(size=10, color=color),
            showlegend=False,
            hovertemplate=(
                f"<b>{year} Peak</b> — {peak['month_day']}<br>"
                f"7-Day Avg: {peak_val:,.0f}<extra></extra>"
            ),
        ))

    fig.update_layout(
        title="NYC Subway Ridership — Year-over-Year (7-Day Rolling Average)",
        xaxis=dict(
            tickmode="array",
            tickvals=MONTH_DAY_STARTS,
            ticktext=MONTH_LABELS,
            title="",
        ),
        yaxis=dict(title="Ridership (7-Day Avg)", tickformat=",.0f"),
        hovermode="x unified",
        legend=dict(x=0.01, y=0.99),
        template="plotly_white",
    )
    return fig


if __name__ == "__main__":
    fig = create_figure()
    DATA_DIR.mkdir(exist_ok=True)
    fig.write_html(str(OUTPUT_FILE))
    print(f"Chart saved to {OUTPUT_FILE}")
    webbrowser.open("file://" + os.path.abspath(OUTPUT_FILE))
