# Spotify Chart Markets -- Research Findings

## Market Structure

Four daily series on Kalshi based on Spotify chart rankings:

| Series | Question | Resolved Markets | Avg Winner Last Price |
|--------|----------|------------------|-----------------------|
| KXSPOTIFYD | "Top USA Song on Spotify on [date]?" | 1,433 | 0.83 |
| KXSPOTIFYGLOBALD | "Top Global Song on Spotify on [date]?" | 1,405 | 0.81 |
| KXSPOTIFY2D | "Runner-up top Song on [date]?" | 1,383 | 0.68 |
| KXSPOTIFYARTISTD | "Top USA Artist on Spotify on [date]?" | 1,141 | 0.68 |

**Format:** Each day has ~15-27 buckets (candidate songs/artists). You bet YES/NO on whether a specific song will be #1 (or #2 for runner-up). Exactly one bucket resolves YES per day.

**Market close:** 11:59 PM Eastern Time on the chart date itself. (Confirmed via DST shift: 04:59 UTC pre-DST, 03:59 UTC post-DST -- both = 11:59 PM ET.)

**No early_price data** -- these markets were not included in our early price pulls.

## Song Persistence (Key Pattern)

The #1 song is highly persistent. Songs typically dominate for multi-day or multi-week streaks:

| Song | Consecutive #1 Days | Period |
|------|---------------------|--------|
| DtMF | 21 days | Feb 5 - Feb 25 |
| End of Beginning | 13 days | Jan 3 - Jan 26 |
| I Just Might | 9 days | Jan 10 - Jan 22 |
| Stateside + Zara Larsson | 8 days | Feb 26 - Mar 20 |
| Risk It All | 6 days | Feb 28 - Mar 6 |
| American Girls | 5 days | Mar 7 - Mar 13 |

**Implication:** The incumbent song wins most days. Transitions (new #1) are the interesting events, and the market is good at pricing these -- the favorite (highest last_price) won on 79 of 88 resolved days (90%). The 10% of days where the favorite lost are where alpha lives.

## Data Source Availability

### Spotify Web API (api.spotify.com)
- **Requires auth** (OAuth2 with developer account, free tier available)
- **Does NOT expose play counts** via API. Spotify removed public stream counts from the API. The desktop/mobile app shows total play counts on track pages, but these are not real-time daily counts -- they are cumulative all-time plays.
- **No chart endpoint** in the public API. There is no `/v1/charts` or similar.
- **Verdict: Not useful for predicting daily charts.**

### charts.spotify.com (Official)
- Returns HTML shell; chart data loaded client-side via authenticated API calls
- Backend endpoint `charts-spotify-com-service.spotify.com/auth/v0/charts/` returns 401 without Spotify session cookies
- The `/public/v0/charts` endpoint returns only chart metadata (names, dates) without actual entries
- **Chart dates observed:** The public metadata showed weekly charts dated 2026-03-26 (5 days ago). Daily charts are behind a login wall.
- **Verdict: Requires Spotify account session; scrapeable but auth-gated.**

### kworb.net (Third-Party Tracker)
- **Fully accessible**, no auth needed
- Provides complete daily chart with rankings
- **Current lag: ~2 days** (shows Mar 29 data on Mar 31)
- This means kworb publishes the chart for day X approximately on day X+2
- **Verdict: Useful as a data source but does NOT provide advance information. The chart appears AFTER the Kalshi market closes.**

## Timing Analysis (The Critical Question)

```
Timeline for "Top USA Song on Mar 27":

Mar 27 00:00 ET  -- Chart measurement period begins (midnight to midnight)
Mar 27 23:59 ET  -- Kalshi market CLOSES (03:59 UTC Mar 28)
Mar 27 23:59 ET  -- Chart measurement period ends
~Mar 28-29       -- Spotify compiles and publishes the daily chart
~Mar 29          -- kworb.net picks up and displays the data
```

**Critical finding: The Kalshi market closes at the SAME TIME as the chart measurement period ends.** There is no gap between "knowing the result" and "market still open" because:

1. Spotify's daily chart measures streams from midnight to midnight (in some timezone, likely UTC or US-based)
2. The market closes at 11:59 PM ET on that same day
3. The chart is not published until the following day (or later)
4. There is no public real-time streaming leaderboard during the day

**This is NOT like weather markets** where you can observe the temperature before the official reading is published. There is no public real-time feed of streaming counts during the day.

### Potential Micro-Edge: Intraday Song Popularity Signals

While there's no direct streaming count feed, there are indirect signals:
- **Spotify "popularity" score** (0-100) available via API -- updates roughly daily, reflects recent listening
- **Social media buzz** (TikTok virality, Twitter mentions)
- **New release detection** (Spotify API does expose new releases, which can cause chart disruption)
- **Apple Music charts** (updated more frequently, correlated with Spotify)

However, given the strong persistence of #1 songs (multi-week streaks), the baseline strategy of "bet the incumbent" already captures most of the edge, and the market prices this in (avg winner last_price of 0.83).

## Backtestability

- **No early_price data** in our DB for Spotify markets
- **Historical chart data:** kworb.net has archives, and Kaggle has Spotify daily chart datasets going back years
- **Forward test possible:** We can scrape kworb.net daily, match to market outcomes, and build a model
- **But:** Without early_price, we cannot backtest profitability (we don't know what price we'd have bought at)
- **Key limitation:** The information structure here is different from weather. There's no "observation before publication" window. Any edge would come from better modeling of song persistence/transition probabilities.

## Runner-Up Market (KXSPOTIFY2D)

Less predictable than #1 -- avg winner last_price of 0.68 (vs 0.83 for #1). The #2 position is more volatile, meaning more potential alpha but also harder to predict. Seven out of ~90 winners had last_price of 0.00 (complete surprises).

## Recommendation: SKIP (no information arb available)

**Rationale:**
1. **No real-time data edge.** Unlike weather markets (where you can read the thermometer), there is no public source of intraday Spotify streaming counts. The Spotify API does not expose play counts, and the chart is published after the market closes.
2. **No observation-before-publication gap.** The market closes at midnight ET, the same time the chart measurement period ends. The chart result is unknown to everyone until Spotify publishes it the next day.
3. **Strong persistence makes it boring.** The #1 song wins for weeks at a time, and the market already prices this (83 cents for the favorite). Buying at 83 and winning at 100 is a 20% return per bet, but the 10% miss rate eats the edge.
4. **Not backtestable** without early_price data or a way to reconstruct historical market prices.
5. **No Spotify developer account** currently set up, and even if we had one, the API does not provide the data we'd need.

**If we revisited:** The only angle worth exploring is building a song-transition model (predicting when the #1 will change) and forward-testing it. This would require:
- Monitoring Apple Music charts (updated more frequently)
- Tracking TikTok viral trends
- Building a "song lifecycle" model based on historical chart data

This is fundamentally a prediction problem, not an information-arb problem. Not our edge.
