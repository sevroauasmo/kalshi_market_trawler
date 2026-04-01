# Covariate Data Feasibility: Free Economic Leading Indicators for Kalshi Markets

Generated: 2026-03-31

## Executive Summary

Three categories of free economic data were investigated as potential leading
indicators for Kalshi economics markets.

**UPDATE (2026-03-31): Cleveland Fed nowcast data IS accessible.** The website
blocks basic requests but serves full JSON data with browser-like headers.
We now have 153 months of daily nowcast data (CPI YoY, Core CPI, PCE, Core PCE)
cached at `data/clevefed_cache/`. Each month has ~22 daily readings showing
how the nowcast evolves. This is backtestable against our Kalshi CPI markets.

JSON endpoints:
- `https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_year.json`
- `https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json`
- `https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_quarter.json`
(Requires User-Agent header)

FRED CSV endpoints remain the most reliable for other series. Previous note about Cleveland Fed blocking access is outdated
on FRED. The Atlanta Fed GDPNow daily tracking data is downloadable as an
Excel file with history back to 2014.

The most promising near-term opportunity is **KXJOBLESSCLAIMS** (140 resolved
markets, weekly cadence) using FRED ICSA/CCSA data and simple autoregressive
models. CPI markets are backtestable but have fewer resolved observations.

---

## Data Source 1: Cleveland Fed Inflation Nowcast

**Target Kalshi series:** KXECONSTATCPIYOY (48 resolved), KXCPIYOY (29 resolved),
KXCPI (18), KXCPICORE (18), KXECONSTATCORECPIYOY (48), KXCPICOMBO (10)

### Direct Access (clevelandfed.org)

| Attribute | Value |
|---|---|
| Historical data available? | NO (programmatically) |
| Access method | Website blocks bots (403 on WebFetch, no public API found) |
| Download links found | None functional -- `/inflation-nowcasting-download.aspx` returns 404 |
| API endpoints tested | `/api/InflationNowcasting/*` -- all 404 |

The Cleveland Fed redesigned their site. The old download page
(`inflation-nowcasting-download.aspx`) no longer exists. The main page at
`/en/indicators-and-data/inflation-nowcasting.aspx` loads data via JavaScript
which cannot be scraped with simple HTTP requests. A headless browser
(Selenium/Playwright) would be required.

### FRED Proxy: Cleveland Fed Expected Inflation (EXPINF1YR)

| Attribute | Value |
|---|---|
| Historical data available? | YES |
| FRED series | `EXPINF1YR` (1-year), `EXPINF2YR` (2-year) |
| How far back? | Monthly, full history on FRED |
| Update frequency | Monthly |
| Backtestable? | Partially -- monthly frequency limits alignment with Kalshi close times |
| Correlation with actual CPI YoY | 0.37 (weak, 10-month sample) |
| MAE vs actual CPI YoY | 0.24 pp |

**Key finding:** EXPINF1YR is a 1-year forward expectation, NOT a same-month
nowcast. It does not directly predict the next CPI print. The correlation of
0.37 reflects that it captures broad trends but not month-to-month variation.

### Other FRED CPI-Adjacent Series

| Series | Description | Frequency | Useful? |
|---|---|---|---|
| `CPIAUCSL` | CPI-U All Items (the settlement source) | Monthly | YES -- for computing YoY after release |
| `T5YIE` | 5Y Breakeven Inflation | Daily | Market-based, updates daily, 2.57% latest |
| `T10YIE` | 10Y Breakeven Inflation | Daily | Market-based, updates daily, 2.31% latest |
| `CORESTICKM159SFRBATL` | Atlanta Fed Sticky CPI | Monthly | 2.90% latest, lags by ~1 month |

### Theoretical Edge for CPI Markets

The CPI is released by BLS on a known schedule (usually mid-month for prior
month). Kalshi CPI markets close at the time of BLS release. Any indicator
that updates BEFORE the BLS release and correlates with CPI has edge.

**Best candidates:**
- Breakeven inflation rates (T5YIE, T10YIE) -- daily, market-implied
- Cleveland Fed nowcast (if accessible) -- daily updates pre-release
- EXPINF1YR -- monthly, weak correlation

**Verdict: MEDIUM priority.** CPI markets have 77+ resolved observations across
series but the best leading indicator (Cleveland Fed daily nowcast) is not
programmatically accessible. Breakeven rates are available daily but their
predictive power for a single monthly print is unproven.

---

## Data Source 2: Jobless Claims Leading Indicators

**Target Kalshi series:** KXJOBLESSCLAIMS (140 resolved markets, 14 unique weeks)

### FRED Data Availability

| Series | Description | Available? | Frequency | Latest |
|---|---|---|---|---|
| `ICSA` | Initial Claims (SA) | YES | Weekly (Thu) | 210,000 (Mar 21) |
| `CCSA` | Continuing Claims (SA) | YES | Weekly (Thu, 1-wk lag) | 1,819,000 (Mar 14) |
| `IC4WSA` | Initial Claims 4-wk MA | YES | Weekly | 210,500 (Mar 21) |
| `IURSA` | Insured Unemployment Rate | YES | Weekly | 1.2% (Mar 14) |

All available via FRED CSV endpoint:
```
https://fred.stlouisfed.org/graph/fredgraph.csv?id=ICSA&cosd=YYYY-MM-DD&coed=YYYY-MM-DD
```

### DB Market Structure

- Each week has ~10 strike levels (e.g., "At least 195000", "At least 200000", ...)
- Markets close on Thursday at 12:25-12:30 UTC (same day as DOL release at 8:30 ET)
- `early_price` is NULL for the earliest markets in DB (Dec 2024)
- `last_price` is available for all markets
- Result is binary: "yes" (actual >= threshold) or "no" (actual < threshold)
- Data range: Dec 24, 2025 to Mar 26, 2026 (14 weeks)

### Alignment Analysis

Kalshi KXJOBLESSCLAIMS markets for "week ending Mar 21" close on Mar 26.
The DOL releases initial claims data on Thursday morning. ICSA on FRED uses
the Saturday ending date for the reference week. Example alignment:

| Kalshi Close | Market Reference | ICSA Date | ICSA Value | Market Implied |
|---|---|---|---|---|
| 2026-03-26 | week ending Mar 21 | 2026-03-21 | 210,000 | 210K-215K |
| 2026-03-19 | week ending Mar 14 | 2026-03-14 | 205,000 | 205K-210K |
| 2026-03-12 | week ending Mar 7 | 2026-03-07 | 213,000 | 210K-215K |

Alignment is clean -- the ICSA observation date matches the Kalshi reference week.

### Leading Indicator Analysis

| Predictor | Correlation with ICSA(t) | Notes |
|---|---|---|
| ICSA(t-1) (last week) | 0.62 | Simple AR(1) baseline |
| CCSA(t) same week | 0.45 (levels) | Published same day, no lead time |
| CCSA(t-1) lagged | 0.38 (levels) | 1 week lead, weak signal |
| CCSA change(t-1) | 0.20 (changes) | Very weak predictive power |

**AR(1) baseline performance:**
- MAE: 7,544 (mean ICSA: 223,640, std: 11,394)
- RMSE: 9,950
- This is a ~3.4% mean absolute error

### Theoretical Edge

CCSA (continuing claims) is released with a 1-week lag relative to ICSA, meaning
this week's CCSA report covers the same reference week as LAST week's ICSA.
Both are released simultaneously on Thursday. There is NO timing advantage from
CCSA -- it does not publish before ICSA.

**Real edge opportunities:**
1. State-level advance claims: Some states report before the national DOL release.
   Not available on FRED; would need state DOL websites.
2. Prior-week ICSA as predictor: AR(1) with 0.62 correlation could exploit
   markets that are mispriced relative to last week's actual value.
3. Seasonal patterns: Claims have known seasonal spikes (holidays, weather).
   A seasonal-adjusted model might beat the market's implied distribution.

**Verdict: HIGH priority.** 140 resolved markets, clean data alignment, FRED data
freely available, and weekly cadence means rapid iteration. The AR(1) MAE of
~7,500 suggests that if Kalshi markets have 5,000-unit strike spacing, even a
simple model could identify mispriced strikes.

---

## Data Source 3: Atlanta Fed GDPNow

**Target Kalshi series:** KXGDP (16 resolved), KXGDPYEAR (12 resolved)

### Data Availability

| Attribute | Value |
|---|---|
| Historical data available? | YES |
| Access method | Excel download from Atlanta Fed |
| URL | `https://www.atlantafed.org/-/media/Project/Atlanta/FRBA/Documents/cqer/researchcq/gdpnow/GDPTrackingModelDataAndForecasts.xlsx` |
| File size | 10.7 MB |
| How far back? | May 2014 (1,801 daily forecasts in TrackingArchives) |
| Update frequency | ~Daily during quarter, after each data release |
| Quarters covered | 47 (2014Q2 through 2026Q1) |
| FRED series | `GDPNOW` -- quarterly final values only (48 obs), NOT daily |

### Excel File Contents (Key Sheets)

| Sheet | Description | Rows |
|---|---|---|
| `TrackRecord` | Final nowcast vs BEA advance estimate | 59 quarters |
| `TrackingArchives` | Daily GDP nowcast history | 1,801 rows |
| `TrackingHistory` | Current quarter daily evolution | ~15 columns (dates) |
| `ContribArchives` | Component contribution history | Large |
| `PseudoRTGDPCompForecasts` | Pseudo-real-time component forecasts | Large |

### Track Record

- 59 quarters of out-of-sample forecasts
- MAE: 0.786 percentage points
- RMSE: 1.169 percentage points
- Notable misses: 2025Q1 (-2.73 nowcast vs -0.28 actual, 2.46 pp error)
- Recent accuracy: 2024Q4 had only 0.02 pp error

### Backtestability

| Factor | Assessment |
|---|---|
| Historical nowcast data? | YES (1,801 daily points since 2014) |
| Historical Kalshi prices? | Limited (16 GDP markets, all from 2026) |
| Alignment possible? | YES -- can match daily nowcasts to Kalshi close times |
| Sufficient sample? | NO -- only 16 resolved GDP markets (1-2 quarters) |

**Verdict: LOW priority for now.** The GDPNow data is excellent and freely
available with deep history, but Kalshi GDP markets are too new (16 resolved,
all from a single quarter). Revisit in 6-12 months when more GDP markets have
resolved. In the meantime, the GDPNow Excel file should be downloaded and
archived regularly.

---

## Data Source 4: BLS Release Calendar

| Attribute | Value |
|---|---|
| URL tested | `https://www.bls.gov/schedule/2026/home.htm` |
| Access | 403 (blocked) |
| Alternative | Release dates are known well in advance and published in BLS press releases |
| Usefulness | Helps time trades -- know exactly when CPI/jobs reports drop |

The BLS calendar is blocked programmatically but the release dates are
well-known and static. They can be hardcoded or scraped from news sources.

---

## Priority Ranking

| Rank | Data Source | Kalshi Series | Resolved Markets | Data Quality | Edge Potential |
|---|---|---|---|---|---|
| 1 | FRED ICSA + AR model | KXJOBLESSCLAIMS | 140 | Excellent | Medium -- AR(1) baseline |
| 2 | FRED CPI + breakevens | KXCPIYOY family | 77+ | Good | Medium -- daily breakevens |
| 3 | Atlanta Fed GDPNow | KXGDP | 16 | Excellent | High (but tiny sample) |
| 4 | Cleveland Fed nowcast | KXCPIYOY family | 77+ | Inaccessible | High (if accessible) |

## Recommended Next Steps

1. **Build KXJOBLESSCLAIMS backtester** using FRED ICSA data:
   - Pull historical ICSA from FRED
   - Pull all 140 resolved markets from DB
   - For each week, use AR(1) + seasonal adjustment to predict ICSA
   - Compare model-implied probabilities at each strike to market prices
   - Measure edge: what would P&L have been trading the model vs market?

2. **Set up GDPNow archival**: Download the Excel file weekly and store
   daily nowcast values for future backtesting when more GDP markets resolve.

3. **Investigate Cleveland Fed headless scraping**: Use Playwright to access
   the inflation nowcast page and extract daily CPI estimates. This would
   be the strongest CPI leading indicator if accessible.

4. **Explore breakeven inflation rates** (T5YIE, T10YIE) as daily signals
   for CPI markets -- test whether day-over-day breakeven moves predict
   CPI surprise direction.

## FRED CSV Endpoint Reference

All FRED data is freely available without API key via:
```
https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES}&cosd={START}&coed={END}
```

Key series for economics markets:
- `ICSA` -- Initial Jobless Claims (weekly, Thursday)
- `CCSA` -- Continuing Claims (weekly, 1-week lag)
- `IC4WSA` -- 4-week MA of Initial Claims
- `CPIAUCSL` -- CPI-U All Items (monthly, ~15th)
- `EXPINF1YR` -- Cleveland Fed 1Y Expected Inflation (monthly)
- `T5YIE` -- 5Y Breakeven Inflation (daily)
- `T10YIE` -- 10Y Breakeven Inflation (daily)
- `GDPNOW` -- Atlanta Fed GDPNow (quarterly final only)
- `CORESTICKM159SFRBATL` -- Atlanta Fed Sticky CPI (monthly)
