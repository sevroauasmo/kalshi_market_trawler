# Kalshi Market Trawler — Analysis Log

## EXECUTIVE SUMMARY (updated 2026-04-01)

**VALIDATED: Second-favorite short = +$6,443 on REAL NO ask prices over 98 days.**

The 2nd-highest-priced temperature bucket is systematically overpriced. Buying NO
at the real ask price (from candlestick bid data) across 6 cities × 5 entry times
produces +$6,443 over 98 winter days on $100 flat bets.

**Best plays:** NYC (+$5,463) and Miami (+$1,921). Best entry: 3pm ET day-before.
NYC+Miami at 3pm = +$2,906/98 days = ~$10,800/year.

**What failed:** Forecast-based approaches (MOS, Prophet, Open-Meteo) don't beat
the market on real execution prices. METAR speed edge priced in by 3pm. CPI nowcast
too few markets to backtest. Spotify no observation window.

**Caveats:** 98 winter days only. NYC drives most P&L. Need summer data to confirm.

**DEFINITIVE RESULT: Second-favorite short on real NO ask prices = +$6,443 over 98 days.**

Full grid (6 cities × 5 entry times, real candlestick NO ask prices, $100 flat bets):
| City | 10am | 11am | 1pm | 3pm | 5pm | Total |
|------|------|------|-----|-----|-----|-------|
| **NYC** | -$109 | +$1,345 | +$856 | **+$1,920** | +$1,450 | **+$5,463** |
| **Miami** | +$334 | -$597 | +$431 | **+$986** | +$767 | **+$1,921** |
| Chicago | +$175 | +$497 | -$30 | -$51 | -$410 | +$181 |
| LA | +$238 | -$366 | +$474 | +$363 | -$608 | +$101 |
| Austin | -$231 | -$140 | +$147 | +$33 | -$336 | -$527 |
| Denver | -$118 | -$393 | -$101 | +$199 | -$283 | -$696 |
| **TOTAL** | +$289 | +$345 | +$1,778 | **+$3,451** | +$579 | **+$6,443** |

Best combo: **NYC + Miami at 3pm ET** = +$2,906 over 98 days = **$29.65/day**
Annualized on just these 2 cities at 3pm: ~**$10,800/year** on $100 flat bets.

Script: `run_second_fav_real.py`

## Forward Test Infrastructure (built 2026-04-01)

**Table:** `kalshi.forward_test` — logs daily signals and tracks outcomes.

**CLI commands:**
- `trawler fwd scan` — scan open markets, log signals
- `trawler fwd settle` — update with actual results after markets close
- `trawler fwd report` — show P&L summary

**First scan (Apr 1):**
- NYC Apr 2: "54° to 55°" SIGNAL (YES=24%, EV=$0.085/dollar)
- Miami Apr 1: "81° to 82°" SIGNAL (YES=30%, EV=$0.027/dollar)
- Austin Apr 2: "82° to 83°" SIGNAL (YES=25%, EV=$0.025/dollar)

**Edge curves (from backtest):**
| City | YES rate | BE NO | Profitable YES range |
|------|----------|-------|---------------------|
| NYC | 15.5% | 85c | 20-35c |
| Austin | 22.5% | 78c | 20-30c |
| Miami | 27.3% | 73c | 20-30c |
| Denver | 26.3% | 74c | 20-28c |
| LA | 23.9% | 76c | 20-30c |
| Chicago | 28.1% | 72c | marginal |

**Optimal sizing simulation running** (`run_optimal_sizing.py`) — computing realistic
P&L with price impact, volume constraints, and position sizing for all 6 cities.

**Key concern:** All analysis is on 98 winter days (Dec 23 - Mar 30). No summer data exists.
The March degradation is a warning sign.

---

## Backtest Results (2026-03-31)

### Weather — Rank-Based Structural Edge (PROMISING)

**Finding: The 4th-favorite bucket is systematically overpriced across ALL 6 cities.**
Market prices it at ~11% but it only wins ~8% of the time. Buying NO on rank_4 is +EV.

**Rank 4 BuyNO results (all 6 cities, 98 days, early_price VWAP):**
| City | Trades | Win% | VWAP P&L | +2c slip | +3c slip |
|------|--------|------|----------|----------|----------|
| NYC | 94 | 92% | +$426 | +$207 | +$103 |
| Austin | 76 | 92% | +$386 | +$206 | +$120 |
| Denver | 72 | 91% | +$353 | +$183 | +$100 |
| Chicago | 96 | 89% | +$299 | +$176 | +$68 |
| Miami | 54 | 96% | +$276 | +$155 | +$94 |
| LA | 69 | 89% | +$76 | -$78 | -$153 |
| **TOTAL** | **461** | **91%** | **+$1,919** | **+$849** | **+$332** |

**Survives 2c slippage on 5/6 cities.** LA doesn't work (only 1pp bias).

**Monthly breakdown shows edge fading:**
- Dec: 100% win rate (15t), Jan: 94% (105t), Feb: 91% (163t), Mar: 90% (178t)
- Could be seasonal or market learning. Need summer data to know.

**Day of week:** Monday worst (-$34), Saturday best (+$443). Weekend less efficient?

**Why this works:** The market spreads too much probability across non-favorite buckets.
6 buckets with ~17% each would be uniform. Market gives rank_4 about 11% — closer to uniform
than it should be, given that the actual distribution is peakier.

**COMBINED STRATEGY: Buy YES rank_1 + Buy NO rank_4 (2c slippage both legs):**
| City | Days | P&L | $/day |
|------|------|-----|-------|
| NYC | 91 | +$1,899 | +$20.90 |
| Austin | 74 | +$1,495 | +$20.20 |
| Denver | 65 | +$1,143 | +$17.60 |
| Miami | 46 | +$718 | +$15.60 |
| Chicago | 91 | -$1,048 | SKIP |
| LA | 65 | -$645 | SKIP |
| **4 cities** | **276** | **+$5,254** | **+$53.60/day** |

Annualized: ~$19,500/year on $200/day deployed capital per city.

**Monthly breakdown (4 profitable cities combined):**
- Dec: +$298 (7 city-days, small sample)
- Jan: +$1,221 (55 city-days)
- Feb: +$4,028 (99 city-days, STRONGEST)
- Mar: -$294 (115 city-days, FADING)

**Buying YES on favorite is the bigger leg:** NYC favorite underpriced by 8.7pp (38.7% price, 47.4% actual).
The favorite win rate varies by city: NYC 47%, MIA 53%, AUS 43%, DEN 38%. All above their market prices.

**⚠ Caveats:**
- Only 98 days of data (Dec 23 - Mar 30), all winter
- Edge fading: strong Dec-Feb, negative in March
- Using early_price (VWAP ~1hr after open) with 2c slippage estimate
- Real NO/YES ask candlestick backtest still running
- Chicago and LA don't show this pattern — city-specific
- Could be overfitting to 4 months of winter data

**March is NEGATIVE** — Dec -$16, Jan +$1,346, Feb +$3,845, Mar -$1,979.
The combined strategy made all its money in Jan-Feb and gave it back in March.
This is concerning — could be seasonal (spring weather harder) or market learning.

**Rank_4 standalone is more robust** — survives 2c slippage on 5/6 cities, 3c on 4/6:
| City | Trades | Win% | +2c P&L | +3c P&L |
|------|--------|------|---------|---------|
| NYC | 94 | 92% | +$208 | +$103 |
| Austin | 76 | 92% | +$207 | +$121 |
| Denver | 72 | 91% | +$183 | +$101 |
| Miami | 54 | 96% | +$155 | +$96 |
| Chicago | 96 | 89% | +$76 | -$32 |
| LA | 69 | 89% | -$77 | -$152 |

**Key rank calibration patterns (early_price VWAP, 97 days):**
| City | R1 bias | R2 bias | R4 bias |
|------|---------|---------|---------|
| NYC | +8.7% | -13.1% | -3.8% |
| Austin | +9.4% | -3.3% | -4.4% |
| Miami | +9.5% | -2.9% | -4.7% |
| Denver | +3.1% | +1.2% | -4.2% |
| Chicago | -5.0% | +2.2% | -2.5% |
| LA | +3.2% | -2.6% | -1.0% |

NYC, Austin, Miami have strongest favorite underpricing (8-10pp).
Rank_4 overpriced in all cities (1-5pp).
Chicago is the outlier — favorite is OVERPRICED there.

**EUR/USD rank calibration shows massive overpricing** (-19pp on R1, -9pp on R3) but only 16 days of early_price data. Needs more data.

**Status: Rank_4 standalone is the most defensible strategy. Small but consistent.
Need summer data and real candlestick ask prices to fully validate.
Main candlestick backtest still running (very slow API).
LOW temp and newer cities early_price pulls in progress.**

---

### Weather — Earlier Approaches (DEAD)
- **Setup:** At 3pm local time, read running max temp from METAR+special obs data. Predict which bucket wins.
- **Multi-city results (98 days each, 3pm cutoff, best correction factor):**
  - NYC: 72% (station=Central Park, same as settlement, +0°F correction)
  - LA: 70% (+1°F), Miami: 67% (+1°F), Denver: 66% (+1°F), Austin: 61% (+1°F), Chicago: 49% (+1°F)
- **NYC is best** because METAR station IS the settlement station. Other cities use airport METARs.
- **Cutoff hour analysis (NYC):** 1pm=44%, 2pm=58%, 3pm=72%, 4pm=73%, 9pm=74%. Plateaus after 4pm.
- **Correction factors DON'T help NYC** (+0°F best). Other cities benefit from +1°F (airport→city offset).
- **~26% of days** the high happens after 3pm or METAR diverges from CLI — this is the ceiling.
- **Net P&L:** +$7.98/98 days at $1 bets. Marginal but safe on lower-tail shorts (100% win rate).
- **ACTUAL P&L (30 days, NYC, $100 bets at 3pm candlestick prices):**
  - Strategy A (buy predicted bucket): 20 wins / 30 bets (67%), **P&L: -$242**
  - Strategy B (only when plateaued): 10/15 (67%), **P&L: -$221**
  - Strategy C (short all wrong buckets): 42/52 (81%), **P&L: -$95**
  - Every price threshold filter is also negative P&L
- **Root cause:** Market is already efficient by 3pm. When we agree with market (bucket >80c), wins are tiny ($1-26). When we disagree (bucket <50c), market is usually right (86%) and we lose $100.
- **Verdict on METAR speed edge:** Market already efficient by 3pm. Not pursuing.
- **NEW: Open-Meteo forecast approach (2-day-ahead, at market open):**
  - Deterministic forecast: 41% bucket accuracy 2 days out (vs 72% same-day METAR)
  - Systematically ~1-2°F low on warm days
  - Open-Meteo Ensemble API gives 31-member probabilistic forecasts → direct bucket probabilities
  - Ensemble NOT available historically (can't backtest), but can forward-test
  - Historical deterministic forecasts available via previous-runs API (backtestable)
  - **Key insight from Polymarket weather winners:** edge likely comes from comparing ensemble probabilities to market prices at open, not from observing actuals
  - **FINAL: BUY NO on real ask prices, 6 cities × 5 entry times = -$17,799 total**
  - NYC 5pm was only green cell (+$883) — outlier, not generalizable
  - Austin worst (-$7,195), Chicago (-$3,238), Denver (-$4,490)
  - GFS MOS + Gaussian uncertainty does NOT beat market makers on execution prices
  - The VWAP backtest (+$6,509) was misleading — bid/ask spread kills the edge
- **Verdict: DEAD.** Market makers are better at weather than a single GFS run.
- **File:** `trawler/backtesting/metar_backtest.py`

### Spotify Information Arb — DEAD END
- Market closes at midnight ET, same time chart period ends. No observation window.
- No public real-time streaming data (Spotify API doesn't expose play counts).
- **Status:** Skip.

### Economics Covariates — CLEVELAND FED CRACKED
- **Cleveland Fed Nowcast: NOW ACCESSIBLE.** JSON endpoints found at:
  - `clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_year.json`
  - Requires browser User-Agent header. 153 months of DAILY nowcast data (CPI YoY, Core CPI, PCE, Core PCE).
  - Each month has ~22 daily readings showing nowcast evolution. Includes actual values.
  - Cached locally at `data/clevefed_cache/`.
  - **Validation:** Dec 2025 nowcast pointed to 2.57% CPI YoY, actual was 2.7%. Close but one bucket off.
  - **Status: NOT BACKTESTABLE — need forward test.** Only 3 months of Kalshi CPI markets overlap with nowcast data (Dec 2025-Feb 2026). Those markets have thin, sporadic volume. Nowcast picked right bucket 2/3 months. Need to forward-test with live markets as more months accumulate.
- **Jobless Claims (KXJOBLESSCLAIMS):** 140 markets, FRED ICSA data clean. AR(1) MAE = 3.4%.
- **Atlanta Fed GDPNow:** Excellent data (1,801 daily nowcasts since 2014) but only 16 Kalshi GDP markets.
- **Next step:** Build CPI nowcast backtester — align daily nowcast with Kalshi market prices, test if nowcast picks the right bucket more often than the market's implied favorite.

### Naive Models vs Early Market Price — NO EDGE
- FRED-based (CPI/gas): +$2,097 on 71 mkts but negative Brier edge
- Prophet (weather): -$3,943 on 696 mkts, market 2x better calibrated
- Calibration-based (all series): Flawed for bucketed markets (mutual exclusivity bug)
- **Conclusion:** Simple models don't beat 1hr-after-open market price

---

## Pipeline Status (2026-03-31)

### Infrastructure
- 9,277 series scanned, 2,218 candidates after filtering
- ~228k resolved markets ingested across 938 series
- 97 series calibrated
- Backtest framework with FRED, Prophet, and calibration models
- Early price (1hr VWAP) pulled for 2,862 markets across 12 priority series

### Key Finding: Naive Models Don't Beat Early Market Prices

Backtested three approaches against `early_price` (VWAP ~1hr after market open):

| Model | Markets | Win Rate | P&L | Brier Edge |
|-------|---------|----------|-----|------------|
| FRED naive (CPI/gas/fed) | 71 | 56.5% | +$2,097 | -0.25 (market better) |
| Prophet (weather 5 cities) | 696 | 65.0% | -$3,943 | -0.15 (market better) |
| Calibration (all series) | 105,767 | 50.7% | mixed | unreliable (bucket correlation bug) |

**Conclusion:** Simple prediction models (historical distributions, Prophet time series) do not beat the market's early trading price. The market is efficient enough at open that we need a structural edge, not a better forecast.

### Previous Bug: Settlement Price Leakage
`last_price` in our DB is the final/settlement price (90% are at 0.01 or 0.99). Early backtests showing 100% win rates on CPI were comparing predictions against the answer, not the question. Fixed by pulling `early_price` via candlestick API.

---

## Edge Taxonomy

### Type 1: Speed Edge — Real-time data faster than settlement source

**Weather Daily Highs/Lows (28 series, 582 mkts each, 14 cities)**
- Markets: "Highest temperature in [City]" — bucketed (e.g., "54° to 55°", "58° or above")
- Settlement: NWS CLI (Climatological) reports — published AFTER the day ends
- Faster source: METAR station data from aviationweather.gov — updates every few minutes
- Lead time: 6-12 hours. By early afternoon you know the day's high temp.
- METAR confirmed working: KJFK 64.9°F, KLAX 64.9°F, KORD 63.0°F live pulls successful
- Backtestability: TBD — need to check if historical METAR is available

**Hourly NYC Temperature Direction (KXTEMPNYCH, 1,911 mkts)**
- Settlement: AccuWeather METAR data
- Faster source: Direct METAR from aviationweather.gov (same underlying data, no AccuWeather delay)

**Gas Prices (KXAAAGASW/M/D, 246 mkts combined)**
- Settlement: EIA weekly average (published Mondays, covers prior week)
- Faster source: GasBuddy real-time, AAA daily fuel gauge
- Monthly gas backtest showed +$1,752 with positive Brier edge (small sample: 17 mkts)

### Type 2: Covariate Edge — Leading indicators predict before settlement

**CPI Cluster (KXECONSTATCPIYOY, KXCPIYOY, etc., ~200 mkts combined)**
- Settlement: BLS CPI release (~mid-month)
- Leading indicators:
  - Cleveland Fed Inflation Nowcast (daily updates, ~2 weeks before BLS)
  - CPI component data (shelter via Zillow, energy via EIA, food via FAO)
  - PPI release leads CPI by ~2 days
- Calibration error: 9.5-15.4%

**Employment (KXECONSTATU3, KXPAYROLLS, KXJOBLESSCLAIMS, ~248 mkts)**
- Settlement: BLS employment situation report
- Leading indicators:
  - ADP private payrolls (2 days before BLS)
  - Weekly initial jobless claims
  - Indeed job postings index
  - ISM employment sub-indices

**PCE Core (KXPCECORE, 25 mkts)**
- Settlement: BEA PCE release
- Leading: CPI components map directly to PCE components (known translation)

### Type 3: Interpretation Edge — Data exists but is hard to consume

**Spotify Daily Charts (4 series, 1,100-1,433 mkts each)**
- Settlement: Published Spotify chart rankings
- Edge: Spotify API gives real-time play counts before chart publication
- Could track streaming velocity to predict daily chart position

**Netflix Weekly Rankings (8 series, 147-164 mkts each)**
- Settlement: netflix.com/tudum/top10 (published Tuesdays)
- Edge: Google Trends + social media buzz + Letterboxd for movies

**Subway Ridership (KXSUBWAY — already exploited, user profited ~$3k)**
- Settlement: MTA data on data.ny.gov
- Edge: YoY analysis makes trend obvious; most participants don't do this

---

## Priority Order for Next Steps

1. **Weather METAR speed edge** — Highest conviction. 28 series, 582 mkts each. Need to:
   - Check if historical METAR is available for backtesting
   - If so: pull historical METAR, align with market open times, backtest
   - If not: set up forward test with live METAR polling

2. **CPI nowcast covariate** — Cleveland Fed publishes daily. Pull historical nowcast data, align with market prices, test predictability.

3. **Spotify real-time** — Check if Spotify API provides streaming counts that lead chart publication. Potentially pure information arb.

4. **Gas real-time** — GasBuddy/AAA data to replace stale FRED. Monthly gas already showed positive edge.
