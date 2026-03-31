"""Fetch MTA subway ridership data from the NY Open Data SODA API."""

import sys
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://data.ny.gov/resource/sayj-mze2.json"
DATA_DIR = Path(__file__).parent.parent / "data"
CACHE_FILE = DATA_DIR / "subway_ridership.csv"


def _parse(raw: list[dict]) -> pd.DataFrame:
    """Parse API response into a clean DataFrame."""
    df = pd.DataFrame(raw)
    df = df.rename(columns={"date": "date", "count": "ridership"})
    df = df[["date", "ridership"]]
    df["date"] = pd.to_datetime(df["date"])
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce")
    df = df.dropna(subset=["ridership"]).sort_values("date").reset_index(drop=True)
    return df


def fetch_all() -> pd.DataFrame:
    """Fetch all historical subway ridership from the API."""
    params = {
        "$limit": 50000,
        "$order": "date ASC",
        "$where": "mode='Subway'",
    }
    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return _parse(resp.json())


def save(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(CACHE_FILE, index=False)


def load() -> pd.DataFrame:
    """Load cached data. Raises FileNotFoundError if no cache exists."""
    if not CACHE_FILE.exists():
        raise FileNotFoundError(f"No cached data at {CACHE_FILE}. Run fetch first.")
    return pd.read_csv(CACHE_FILE, parse_dates=["date"])


def update() -> pd.DataFrame:
    """Load cache, fetch only newer rows, merge, and save."""
    if CACHE_FILE.exists():
        cached = load()
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
            print("Already up to date.")
            df = cached
    else:
        print("No cache found, fetching all data...")
        df = fetch_all()

    df = df.sort_values("date").reset_index(drop=True)
    save(df)
    return df


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "update"
    if mode == "full":
        df = fetch_all()
        save(df)
    else:
        df = update()
    print(f"Saved {len(df)} rows to {CACHE_FILE}")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")
