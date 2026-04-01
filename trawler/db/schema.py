import logging

from trawler.db.connection import get_connection

log = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS kalshi;

CREATE TABLE IF NOT EXISTS kalshi.series_catalog (
    ticker TEXT PRIMARY KEY,
    title TEXT,
    frequency TEXT,
    category TEXT,
    tags TEXT[],
    settlement_sources JSONB,
    total_volume NUMERIC,
    last_scanned_at TIMESTAMPTZ,
    candidate_status TEXT DEFAULT 'unscreened',
    rejection_reason TEXT,
    llm_assessment JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kalshi.historical_resolutions (
    market_ticker TEXT PRIMARY KEY,
    series_ticker TEXT REFERENCES kalshi.series_catalog(ticker),
    event_ticker TEXT,
    title TEXT,
    yes_sub_title TEXT,
    no_sub_title TEXT,
    result TEXT,
    last_price NUMERIC,
    volume NUMERIC,
    open_interest NUMERIC,
    open_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    expiration_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE kalshi.historical_resolutions ADD COLUMN IF NOT EXISTS early_price NUMERIC;
ALTER TABLE kalshi.historical_resolutions ADD COLUMN IF NOT EXISTS early_price_time TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS kalshi.calibration_scores (
    series_ticker TEXT PRIMARY KEY REFERENCES kalshi.series_catalog(ticker),
    total_markets_resolved INTEGER,
    avg_calibration_error NUMERIC,
    worst_bucket TEXT,
    calibration_curve JSONB,
    last_computed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS kalshi.opportunities (
    market_ticker TEXT PRIMARY KEY,
    series_ticker TEXT REFERENCES kalshi.series_catalog(ticker),
    market_title TEXT,
    market_price NUMERIC,
    our_estimate NUMERIC,
    edge NUMERIC,
    confidence TEXT,
    reasoning TEXT,
    underlying_data JSONB,
    recommended_side TEXT,
    recommended_position NUMERIC,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS kalshi.forward_test (
    id SERIAL PRIMARY KEY,
    test_date DATE NOT NULL,
    city TEXT NOT NULL,
    series_ticker TEXT,
    target_date DATE NOT NULL,
    bucket_sub TEXT NOT NULL,
    bucket_rank INTEGER,
    signal_time TIMESTAMPTZ,
    yes_price_at_signal NUMERIC,
    no_ask_at_signal NUMERIC,
    hourly_volume NUMERIC,
    position_size NUMERIC,
    estimated_fill_no NUMERIC,
    break_even_no NUMERIC,
    ev_per_dollar NUMERIC,
    action TEXT,
    actual_result TEXT,
    actual_pnl NUMERIC,
    settled_at TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kalshi.backtest_results (
    market_ticker TEXT PRIMARY KEY,
    series_ticker TEXT REFERENCES kalshi.series_catalog(ticker),
    market_title TEXT,
    yes_sub_title TEXT,
    close_time TIMESTAMPTZ,
    result TEXT,
    market_price NUMERIC,
    our_estimate NUMERIC,
    edge NUMERIC,
    our_brier NUMERIC,
    mkt_brier NUMERIC,
    pnl NUMERIC,
    bet_taken BOOLEAN,
    model_version TEXT DEFAULT 'v1_naive',
    backtested_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def init_schema():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        log.info("kalshi schema initialized")
    finally:
        conn.close()
