---
title: "Predicting Every At-Bat: How Our Matchup Model Works"
date: 2026-03-09
author: Nick Labella
slug: matchup-model-explained
---

<div class="article-hero">
  <div class="article-hero-label">Basenerd Research</div>
  <h1 class="article-hero-title">Predicting Every At-Bat</h1>
  <div class="article-hero-subtitle">How Our XGBoost Matchup Model Predicts PA Outcomes in Real Time</div>
</div>

Every plate appearance in baseball is a collision of tendencies. A batter who swings and misses at breaking balls. A pitcher who commands the zone. A park that inflates home runs. A bases-loaded, two-out situation that changes everything.

We built a machine learning model that synthesizes all of these factors into a single prediction: **what is the probability of every possible outcome of this plate appearance?** The model runs live on our gamecast during every at-bat, giving you a real-time window into what the matchup looks like before the first pitch is thrown.

## What the Model Predicts

For each batter-pitcher matchup, the model outputs probabilities across nine outcome classes:

| Outcome | Description |
|---------|-------------|
| **K** | Strikeout |
| **OUT** | Ball in play, recorded out (including DPs, sac flies) |
| **BB** | Walk |
| **HBP** | Hit by pitch |
| **IBB** | Intentional walk |
| **1B** | Single |
| **2B** | Double |
| **3B** | Triple |
| **HR** | Home run |

From these raw probabilities, we derive familiar summary stats: **xAVG** (expected batting average), **xSLG** (expected slugging), **xOBP** (expected on-base percentage), along with K% and BB%.

These are not season-long projections. They are **matchup-specific probabilities** -- how this particular batter is expected to perform against this particular pitcher, in this park, in this inning, with these runners on base.

## The Model Architecture

The model is an **XGBoost gradient-boosted tree classifier** trained on 730,089 plate appearances from the 2021-2024 MLB seasons and tested on 182,840 PAs from 2025. It uses the `multi:softprob` objective to output calibrated probabilities across all nine classes simultaneously.

Key training parameters:

- **409 boosting rounds** (early-stopped from 500 max)
- **Max depth: 4** -- shallow trees that generalize well and resist overfitting
- **Learning rate: 0.1** with 80% subsampling
- **Min child weight: 100** -- each leaf must represent at least 100 plate appearances, preventing the model from memorizing rare matchup noise
- **L1/L2 regularization** (alpha=0.1, lambda=1.0)

The model achieves a **test log loss of 1.442** on the held-out 2025 season. For context, a model that just predicts league-average rates for every PA scores about 1.48, so the matchup model captures meaningful batter/pitcher-specific signal beyond base rates.

## The 43 Features

The model ingests 43 numeric features and 2 categorical features, organized into five groups.

### Batter Profile (17 features)

These are full-season aggregate stats for the batter, capturing their overall offensive identity:

| Feature | Description | Why It Matters |
|---------|-------------|----------------|
| `bat_k_pct` | Strikeout rate | Primary driver of K probability |
| `bat_bb_pct` | Walk rate | Primary driver of BB probability |
| `bat_whiff_rate` | Swing-and-miss rate | Measures bat-to-ball skill |
| `bat_chase_rate` | Chase rate (swings at pitches outside the zone) | Discipline indicator |
| `bat_zone_swing_rate` | Swing rate on pitches in the zone | Aggressiveness |
| `bat_zone_contact_rate` | Contact rate on in-zone swings | Contact quality |
| `bat_avg_ev` | Average exit velocity | Raw power signal |
| `bat_avg_la` | Average launch angle | Fly ball/ground ball tendency |
| `bat_barrel_rate` | Barrel rate | Optimal contact frequency |
| `bat_hard_hit_rate` | Hard-hit rate (95+ mph) | Solid contact frequency |
| `bat_sweet_spot_rate` | Sweet spot rate (8-32 degree LA) | Productive contact |
| `bat_gb_rate` | Ground ball rate | Batted ball profile |
| `bat_fb_rate` | Fly ball rate | Batted ball profile |
| `bat_hr_per_fb` | HR per fly ball | Power efficiency |
| `bat_iso` | Isolated power | Extra-base hit frequency |
| `bat_babip` | BABIP | Quality of contact + speed |
| `bat_xwoba` | Expected wOBA | Overall expected production |

### Batter Platoon Split (7 features)

These same-hand or opposite-hand splits capture how the batter performs specifically against the pitcher's throwing arm:

| Feature | Description |
|---------|-------------|
| `bat_plat_k_pct` | K% vs this handedness |
| `bat_plat_bb_pct` | BB% vs this handedness |
| `bat_plat_whiff_rate` | Whiff rate vs this handedness |
| `bat_plat_chase_rate` | Chase rate vs this handedness |
| `bat_plat_avg_ev` | Exit velocity vs this handedness |
| `bat_plat_barrel_rate` | Barrel rate vs this handedness |
| `bat_plat_xwoba` | xwOBA vs this handedness |

<div class="article-callout article-callout-highlight">
<strong>The platoon K% is the single most important feature in the entire model</strong> (12.3% of total feature importance). How a batter handles same-side or opposite-side pitching is the strongest signal for predicting PA outcomes.
</div>

### Pitcher Profile (9 features)

Aggregate pitcher characteristics derived from their full pitch arsenal:

| Feature | Description |
|---------|-------------|
| `p_avg_stuff_plus` | Average BNStuff+ across all pitch types |
| `p_avg_control_plus` | Average BNCtrl+ across all pitch types |
| `p_avg_velo` | Pitch-count-weighted average velocity |
| `p_whiff_rate` | Overall swing-and-miss rate |
| `p_chase_rate` | Ability to generate swings outside the zone |
| `p_zone_rate` | Percentage of pitches thrown in the strike zone |
| `p_xwoba` | Expected wOBA allowed |
| `p_num_pitches` | Number of distinct pitch types in arsenal |
| `p_total_thrown` | Total pitches thrown in the season (workload) |

### Park Factors (2 features)

| Feature | Description |
|---------|-------------|
| `park_run_factor` | Overall run factor (>1 = hitter-friendly) |
| `park_hr_factor` | HR-specific factor (Coors > Petco) |

Park factors are computed per-venue, per-season from Statcast data, regressed toward 1.0 based on sample size to avoid small-sample noise in newer parks.

### Game Context (6 features)

| Feature | Description |
|---------|-------------|
| `inning` | Current inning (1-9+) |
| `outs_when_up` | Outs in the inning (0, 1, 2) |
| `n_thruorder_pitcher` | Times through the order for the pitcher (1, 2, 3+) |
| `runner_on_1b` | Runner on first (0/1) |
| `runner_on_2b` | Runner on second (0/1) |
| `runner_on_3b` | Runner on third (0/1) |

### Categorical Features (2)

| Feature | Values |
|---------|--------|
| `stand` | L (left) or R (right) -- batter's hitting side |
| `p_throws` | L (left) or R (right) -- pitcher's throwing arm |

## Feature Importance: What Drives the Predictions?

Not all features contribute equally. Here are the top 15 features ranked by their importance in the model's decision-making:

| Rank | Feature | Importance | Category |
|------|---------|-----------|----------|
| 1 | `bat_plat_k_pct` | 12.3% | Batter Platoon |
| 2 | `runner_on_2b` | 7.8% | Context |
| 3 | `runner_on_3b` | 6.5% | Context |
| 4 | `bat_plat_bb_pct` | 6.4% | Batter Platoon |
| 5 | `bat_k_pct` | 5.5% | Batter Overall |
| 6 | `runner_on_1b` | 4.7% | Context |
| 7 | `p_whiff_rate` | 4.1% | Pitcher |
| 8 | `bat_hr_per_fb` | 3.3% | Batter Overall |
| 9 | `inning` | 2.9% | Context |
| 10 | `bat_babip` | 2.9% | Batter Overall |
| 11 | `bat_iso` | 2.8% | Batter Overall |
| 12 | `bat_plat_xwoba` | 2.3% | Batter Platoon |
| 13 | `p_throws` | 2.2% | Categorical |
| 14 | `bat_bb_pct` | 2.2% | Batter Overall |
| 15 | `n_thruorder_pitcher` | 2.1% | Context |

Several patterns jump out:

**Platoon splits dominate.** Three of the top five features are platoon-specific stats. The model has learned that how a batter performs against a specific handedness is more predictive than their overall numbers. A switch hitter facing a lefty specialist has a very different profile than the same hitter facing a right-hander.

**Context is king.** Runners on base collectively account for 19% of total importance -- the largest single category. The model recognizes that pitcher behavior changes dramatically with runners in scoring position (more careful, more walks, different pitch selection) and that defensive positioning shifts affect BABIP.

**Pitcher whiff rate is the top pitching feature** at 4.1%. Interestingly, BNStuff+ and BNCtrl+ show zero importance -- not because stuff and command don't matter, but because their signal is already captured by the rate stats (whiff rate, zone rate, chase rate, xwOBA) which are downstream consequences of stuff and command. The model prefers the observed outcomes over the modeled inputs.

**Power features matter for HR prediction.** HR/FB (3.3%) and ISO (2.8%) are how the model distinguishes between contact hitters who spray singles and power hitters who elevate.

## How It Works in Practice

Let's walk through three real matchups from today's Spring Training games.

### Giancarlo Stanton vs Jose Urquidy

**2nd inning, 0 out, bases empty -- PIT @ NYY**

| Outcome | Probability |
|---------|------------|
| K | 29.5% |
| OUT | 24.4% |
| BB | 9.9% |
| 1B | 10.1% |
| 2B | 4.9% |
| HR | **19.4%** |

**xAVG: .391 | xSLG: 1.109 | xOBP: .461**

The model sees Stanton as an extreme outcome hitter: nearly a 1-in-5 chance of a home run, but also a 29.5% strikeout probability. His ISO and HR/FB rate are elite, and Urquidy's profile (low whiff rate, hittable stuff) makes this a favorable power matchup. The xSLG over 1.100 is the highest we've seen.

**Actual result: Home Run.** The model's most likely non-out outcome.

### Endy Rodriguez vs Max Fried

**5th inning, 0 out, bases empty -- PIT @ NYY**

| Outcome | Probability |
|---------|------------|
| K | **38.8%** |
| OUT | 39.7% |
| BB | 7.0% |
| 1B | 9.5% |
| 2B | 4.0% |
| HR | 0.1% |

**xAVG: .150 | xSLG: .200 | xOBP: .215**

A brutal matchup for Rodriguez. A right-handed hitter facing one of the best left-handed pitchers in baseball. The model gives him a 78.5% chance of making an out (K + OUT combined), a .150 expected batting average, and essentially zero home run probability.

**Actual result: Home Run.** That's baseball -- low-probability events happen, and that's exactly why showing the probabilities is valuable. When Rodriguez goes deep off Fried, you can appreciate just how unlikely it was.

### Nick Sogard vs Bryse Wilson

**3rd inning, 2 out, runners on 1st and 2nd -- PHI @ BOS**

| Outcome | Probability |
|---------|------------|
| K | 14.4% |
| OUT | 47.3% |
| BB | 7.2% |
| 1B | **19.7%** |
| 2B | 8.5% |
| HR | 0.1% |

**xAVG: .327 | xSLG: .459 | xOBP: .383**

The model sees Sogard as a contact-first hitter -- his 14.4% K rate is well below average, and his most likely positive outcome is a single (19.7%). With runners on 1st and 2nd and 2 outs, the context features shift probabilities: pitchers tend to be more careful (higher BB%) and batters are more selective.

**Actual result: 3-Run Home Run.** Another model upset. The .327 xAVG tells you Sogard makes good contact, but the model didn't see power in his profile. Spring training surprises.

## Important Considerations

### What the Model Can and Cannot Do

**It can** predict the probability landscape of a plate appearance based on the full set of available information: who's batting, who's pitching, the platoon matchup, the park, the game situation, and the pitcher's workload.

**It cannot** account for:

- **Day-of adjustments.** If a pitcher is tipping his slider or a batter tweaked his swing in the cage, the model doesn't know.
- **Specific pitch sequencing.** The model works at the PA level, not the pitch level. It doesn't know if the count is 0-2 or 3-1.
- **Defensive alignment.** Shifts, positioning, and defensive quality affect BABIP but aren't in the feature set.
- **Weather.** Wind, temperature, and humidity affect ball flight. We include park factors (which partially capture average weather), but not game-day conditions.
- **Spring Training context.** Players may be working on new pitches, sitting on certain counts, or playing at reduced effort. Spring Training PAs are inherently noisier.

### Calibration and Log Loss

The model's test log loss of **1.442** on the 2025 season is a modest improvement over the ~1.48 you get from just predicting league averages for everyone. This is expected -- **individual plate appearances are inherently unpredictable**. Even the best hitter in baseball makes an out roughly 60% of the time, and the difference between a .250 hitter and a .300 hitter is only one extra hit per 20 at-bats.

The value of the model isn't in predicting individual outcomes (nobody can do that). It's in identifying **where the probability distribution shifts**. Stanton vs a soft-tossing righty has a fundamentally different outcome landscape than a contact hitter facing a high-spin lefty, and the model captures those differences.

### Fallback Behavior

When a batter or pitcher doesn't have enough data in our system (common for rookies, September call-ups, or early in the season), the model falls back to **league-average profiles**. This produces sensible baseline predictions (~.261 xBA, ~.317 xOBP) rather than breaking. As the season progresses and we accumulate pitch-level data, the predictions become more player-specific.

### Season Data

The model currently uses **2025 season profiles** for feature lookups, which is the most recent complete season. During the 2026 regular season, we'll transition to using current-year data as sample sizes become reliable (typically by late April).

## Training Data Pipeline

Building the matchup model requires four pre-computed datasets, each generated from our Statcast database:

1. **Batter Profiles** -- Per-batter, per-season aggregate stats (K%, BB%, whiff rate, exit velocity, etc.) split by overall and by opposing pitcher handedness
2. **Pitcher Arsenal** -- Per-pitcher, per-season aggregate stats (stuff+, control+, velocity, whiff rate, zone rate) weighted across all pitch types
3. **Park Factors** -- Per-venue, per-season run and HR factors, regressed toward 1.0 based on sample size
4. **PA-Level Training Data** -- One row per plate appearance with the batter profile, pitcher profile, park factor, and game context features joined together

The model is retrained periodically as new seasons of data accumulate, using a strict **temporal split** (train on older seasons, test on the most recent) to prevent data leakage.

## What's Next

This is the first version of the matchup model. Future improvements we're exploring:

- **Count-level predictions** -- shifting probabilities as the count changes (0-2 is a very different situation than 3-0)
- **Pitcher fatigue modeling** -- adjusting predictions based on pitch count and velocity trends within the game
- **Defensive quality** -- incorporating team defense (OAA, DRS) to better predict BABIP outcomes
- **Historical H2H data** -- using actual batter-vs-pitcher history when sample size permits
- **Catcher framing** -- the catcher behind the plate meaningfully shifts K/BB rates

The matchup probability panel is live on all gamecast pages during games. Check it out next time you're watching.
