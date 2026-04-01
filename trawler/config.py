import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATABASE_URL = os.environ.get("KALSHI_DB_URL", "")
KALSHI_PRIVATE_KEY_PATH = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_RATE_LIMIT = 10  # requests per second (conservative to avoid 429s)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
