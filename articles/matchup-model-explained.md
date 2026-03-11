---
title: "Predicting Every At-Bat: How Our Matchup Model Works"
date: 2026-03-10
author: Nick Labella
slug: matchup-model-explained
---

<div class="article-hero">
  <div class="article-hero-label">Basenerd Research</div>
  <h1 class="article-hero-title">Predicting Every At-Bat</h1>
  <div class="article-hero-subtitle">How Our XGBoost Matchup Model Uses 86 Features, Pitcher Arsenals, and Real-Time Bayesian Updating to Predict PA Outcomes</div>
</div>

Every plate appearance in baseball is a collision of tendencies. A batter who can't lay off breaking balls. A pitcher whose slider generates 40% whiffs. A park that inflates home runs. A bases-loaded, two-out situation that changes everything. And a batter who's been scorching the ball for two weeks straight.

We built a machine learning model that synthesizes all of these factors into a single prediction: **what is the probability of every possible outcome of this plate appearance?** The model runs live on our gamecast during every at-bat and powers our **pregame predictions page**, where you can see projected outcomes for every batter in both lineups before the first pitch is thrown.

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

These are not season-long projections. They are **matchup-specific probabilities** -- how this particular batter is expected to perform against this particular pitcher, in this park, in this inning, with these runners on base, and with both players' recent performance factored in.

## The Model Architecture

The model is an **XGBoost gradient-boosted tree classifier** trained on 730,089 plate appearances from the 2021-2024 MLB seasons and tested on 182,840 PAs from 2025. It uses the `multi:softprob` objective to output calibrated probabilities across all nine classes simultaneously.

Key training parameters:

- **436 boosting rounds** (early-stopped from 500 max)
- **Max depth: 5** -- enough depth to capture pitch-type interactions
- **Learning rate: 0.05** with 80% subsampling
- **Min child weight: 100** -- each leaf must represent at least 100 plate appearances
- **L1/L2 regularization** (alpha=0.1, lambda=1.0)

The model achieves a **test log loss of 1.4419** on the held-out 2025 season.

## The 86 Features

The model ingests 84 numeric features and 2 categorical features, organized into eight groups. This is a significant expansion from the original 45-feature model, driven by three new feature families: **pitch-type-specific batter performance**, **pitcher arsenal breakdowns**, and **rolling 14-day recent form**.

### Batter Profile (17 features)

Full-season aggregate stats capturing the batter's overall offensive identity:

| Feature | Description | Why It Matters |
|---------|-------------|----------------|
| `bat_k_pct` | Strikeout rate | Primary driver of K probability |
| `bat_bb_pct` | Walk rate | Primary driver of BB probability |
| `bat_whiff_rate` | Swing-and-miss rate | Bat-to-ball skill |
| `bat_chase_rate` | Chase rate (swings outside zone) | Discipline |
| `bat_zone_swing_rate` | Swing rate on in-zone pitches | Aggressiveness |
| `bat_zone_contact_rate` | Contact rate on in-zone swings | Contact quality |
| `bat_avg_ev` | Average exit velocity | Raw power |
| `bat_avg_la` | Average launch angle | Fly ball/ground ball tendency |
| `bat_barrel_rate` | Barrel rate | Optimal contact frequency |
| `bat_hard_hit_rate` | Hard-hit rate (95+ mph) | Solid contact |
| `bat_sweet_spot_rate` | Sweet spot rate (8-32 degree LA) | Productive contact |
| `bat_gb_rate` | Ground ball rate | Batted ball profile |
| `bat_fb_rate` | Fly ball rate | Batted ball profile |
| `bat_hr_per_fb` | HR per fly ball | Power efficiency |
| `bat_iso` | Isolated power | Extra-base hit frequency |
| `bat_babip` | BABIP | Contact quality + speed |
| `bat_xwoba` | Expected wOBA | Overall expected production |

### Batter Platoon Split (7 features)

Same-hand or opposite-hand splits capturing how the batter performs against the pitcher's throwing arm:

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

### Batter vs Pitch-Type Category (15 features) -- NEW

How the batter performs against each category of pitch: **fastballs** (4-seam, sinker, cutter), **breaking balls** (slider, curveball, sweeper, slurve), and **offspeed** (changeup, splitter).

For each category, we track five rate stats:

| Stat | Per Category |
|------|-------------|
| `bvpt_whiff_rate_{cat}` | Whiff rate against this pitch category |
| `bvpt_chase_rate_{cat}` | Chase rate against this pitch category |
| `bvpt_zone_contact_rate_{cat}` | Zone contact rate against this pitch category |
| `bvpt_hard_hit_rate_{cat}` | Hard-hit rate against this pitch category |
| `bvpt_xwoba_{cat}` | xwOBA against this pitch category |

This is crucial because **not all batters struggle with the same pitches**. A batter who mashes fastballs but whiffs at 40% on breaking balls is a very different matchup against a slider-heavy pitcher than against a fastball-dominant one.

### Pitch-Weighted Composite (5 features) -- NEW

The model's most sophisticated feature group. For each batter stat, we compute a **weighted average based on the opposing pitcher's actual pitch mix**:

```
bvpt_w_whiff_rate = (batter_whiff_vs_FB × pitcher_FB_usage) +
                    (batter_whiff_vs_BRK × pitcher_BRK_usage) +
                    (batter_whiff_vs_OFF × pitcher_OFF_usage)
```

If a batter has a .380 xwOBA against fastballs but .200 against breaking balls, and the opposing pitcher throws 60% breaking balls, this composite captures the true matchup quality in a single number.

### Pitcher Arsenal Profile (22 features) -- EXPANDED

We break the pitcher's profile into three tiers:

**Aggregate stats (9 features):** Overall BNStuff+, BNCtrl+, velocity, whiff rate, chase rate, zone rate, xwOBA, pitch count, workload.

**Category usage (3 features):** What percentage of pitches are fastballs, breaking balls, and offspeed. A pitcher who throws 70% breaking balls creates a very different matchup than one who's 70% fastballs.

**Top-3 pitch stats (12 features):** For the pitcher's three most-used pitches, we include individual usage, velocity, whiff rate, and BNStuff+. This lets the model learn that a pitcher whose best pitch is a 96 mph 4-seamer with 130 BNStuff+ is a different animal than one whose best pitch is a sweeper.

| Feature | Description |
|---------|-------------|
| `p_pitch1_usage` | Usage rate of primary pitch |
| `p_pitch1_velo` | Velocity of primary pitch |
| `p_pitch1_whiff` | Whiff rate of primary pitch |
| `p_pitch1_stuff` | BNStuff+ of primary pitch |
| `p_pitch2_*` | Same stats for secondary pitch |
| `p_pitch3_*` | Same stats for tertiary pitch |

### Recent Form -- Rolling 14-Day (11 features) -- NEW

Season-long stats are a starting point, but they miss **hot and cold streaks**. A batter who's hit .400 with a .450 xwOBA over the last two weeks is a different threat than his .260 season line suggests.

**Batter recent form (6 features):**

| Feature | Description |
|---------|-------------|
| `bat_r14_k_pct` | K rate over last 14 days |
| `bat_r14_bb_pct` | Walk rate over last 14 days |
| `bat_r14_xwoba` | xwOBA over last 14 days |
| `bat_r14_barrel_rate` | Barrel rate over last 14 days |
| `bat_r14_whiff_rate` | Whiff rate over last 14 days |
| `bat_r14_chase_rate` | Chase rate over last 14 days |

**Pitcher recent form (5 features):**

| Feature | Description |
|---------|-------------|
| `p_r14_k_pct` | K rate over last 14 days |
| `p_r14_bb_pct` | Walk rate over last 14 days |
| `p_r14_xwoba` | xwOBA allowed over last 14 days |
| `p_r14_whiff_rate` | Whiff rate over last 14 days |
| `p_r14_chase_rate` | Chase rate over last 14 days |

These are computed as rolling windows from our Statcast database. For training data, we use a strict look-back approach -- each PA only sees form data from *before* that game date, preventing look-ahead bias. If a player has fewer than 20 pitches in their 14-day window (injury return, early season), we fall back to league averages.

### Park Factors (2 features)

| Feature | Description |
|---------|-------------|
| `park_run_factor` | Overall run factor (>1 = hitter-friendly) |
| `park_hr_factor` | HR-specific factor (Coors > Petco) |

### Game Context (6 features)

| Feature | Description |
|---------|-------------|
| `inning` | Current inning (1-9+) |
| `outs_when_up` | Outs in the inning (0, 1, 2) |
| `n_thruorder_pitcher` | Times through the order for the pitcher |
| `runner_on_1b` | Runner on first (0/1) |
| `runner_on_2b` | Runner on second (0/1) |
| `runner_on_3b` | Runner on third (0/1) |

### Categorical Features (2)

| Feature | Values |
|---------|--------|
| `stand` | L (left) or R (right) -- batter's hitting side |
| `p_throws` | L (left) or R (right) -- pitcher's throwing arm |

## Feature Importance: What Drives the Predictions?

The top 15 features ranked by importance in the model:

| Rank | Feature | Importance | Category |
|------|---------|-----------|----------|
| 1 | `bat_plat_k_pct` | 12.3% | Batter Platoon |
| 2 | `bat_k_pct` | 6.7% | Batter Overall |
| 3 | `bat_plat_bb_pct` | 5.9% | Batter Platoon |
| 4 | `runner_on_2b` | 5.5% | Context |
| 5 | `p_whiff_rate` | 4.2% | Pitcher |
| 6 | `runner_on_3b` | 4.0% | Context |
| 7 | `runner_on_1b` | 3.2% | Context |
| 8 | `bat_hr_per_fb` | 2.5% | Batter Overall |
| 9 | `inning` | 2.2% | Context |
| 10 | `bat_babip` | 2.2% | Batter Overall |
| 11 | `bat_iso` | 2.1% | Batter Overall |
| 12 | `p_xwoba` | 1.7% | Pitcher |
| 13 | `p_throws` | 1.7% | Categorical |
| 14 | `bat_bb_pct` | 1.7% | Batter Overall |
| 15 | `bat_plat_xwoba` | 1.6% | Batter Platoon |

Several patterns emerge:

**Platoon splits dominate.** Three of the top five features are platoon-specific or handedness-related. The model has learned that how a batter performs against a specific handedness is more predictive than their overall numbers.

**Context matters significantly.** Runners on base collectively account for ~13% of total importance. The model recognizes that pitcher behavior changes with runners in scoring position.

**Pitcher whiff rate is the top pitching feature** at 4.2%. The rate stats (whiff, zone, chase) are more predictive than BNStuff+/BNCtrl+ because they capture the downstream outcomes directly.

**The pitch-type features are distributed.** Rather than any single bvpt feature dominating, the pitch-weighted composites and per-category stats collectively contribute meaningful signal -- they refine predictions when a batter has clear pitch-type weaknesses that align with the pitcher's arsenal.

## Bayesian In-Game Updating

The model doesn't just make static predictions. During live games, it applies **Bayesian adjustments** based on what's actually happening on the mound:

### Velocity Adjustment

We track the pitcher's average fastball velocity tonight compared to their season average. Each 1 mph deviation triggers a proportional adjustment:

- **Throwing harder than expected:** K probability increases, HR probability decreases (faster = harder to square up)
- **Throwing softer than expected:** K probability decreases, HR and BB probability increase (less velocity = more hittable, possibly tiring)

Only activated when the delta exceeds 0.3 mph to avoid noise.

### Fatigue Curve

After 75 pitches, the model applies progressive fatigue adjustments:

- K probability decreases (up to 8% reduction by 100 pitches)
- BB probability increases (up to 6% increase)
- HR probability increases (up to 4% increase)

These factors are applied as multipliers on the raw XGBoost probabilities, then renormalized to sum to 1.0. The adjustments are displayed on the gamecast so you can see exactly how the live context is shifting the prediction from the pregame baseline.

## Pregame Predictions

Before lineups are even posted, you can visit the **pregame predictions page** for any game. Once lineups drop (typically 1-3 hours before first pitch), the page shows:

- **Every batter's predicted outcomes** against the opposing starter
- **HR%, K%, Hit%, and OBP** for each lineup spot
- **Hot/cold indicator** based on 14-day rolling xwOBA (green arrow = hot, red = cold)
- **Pitcher arsenal breakdown** showing pitch mix, velocity, and BNStuff+
- **Team totals** -- expected strikeouts, home runs, hits, and walks for the full lineup

Click any batter's row to expand the full probability bar chart showing their complete outcome distribution.

This is designed for fans who want to understand matchups before the game and bettors looking for edges on player props. When the model shows a 6.2% HR probability (roughly 1 in 16) and the market is pricing higher or lower, that's actionable information.

## Pitch Selection Model

Alongside the matchup model, we trained a separate **XGBoost pitch selection model** on 3.5 million individual pitches. This model predicts what pitch type a pitcher is likely to throw given:

- The count (balls/strikes)
- The game situation (runners, outs, inning)
- Batter handedness
- Previous pitch type
- The pitcher's full arsenal usage rates

The pitch selection model's predictions feed into the matchup model by providing **context-aware pitch usage weights** rather than simple season-average arsenal rates. When it's 0-2, the model knows a pitcher is more likely to throw his put-away pitch than his get-me-over fastball.

## Outcome Rate Calibration

The model's predicted aggregate rates closely match actual rates in the 2025 test set:

| Outcome | Actual Rate | Predicted Rate | Delta |
|---------|------------|----------------|-------|
| K | 22.2% | 22.2% | -0.0% |
| OUT | 46.4% | 46.8% | +0.4% |
| BB | 8.1% | 7.6% | -0.5% |
| 1B | 14.3% | 14.3% | +0.0% |
| 2B | 4.2% | 4.4% | +0.2% |
| 3B | 0.3% | 0.4% | +0.1% |
| HR | 3.1% | 3.1% | +0.0% |
| HBP | 1.1% | 1.0% | -0.1% |
| IBB | 0.3% | 0.3% | +0.0% |

The calibration is excellent -- predicted rates are within 0.5 percentage points of actual rates across all outcome types. This means when the model says there's a 5% chance of a HR, roughly 5% of those situations historically produced home runs.

## Important Considerations

### What the Model Can and Cannot Do

**It can** predict the probability landscape of a plate appearance based on: who's batting, who's pitching, the platoon matchup, the park, the game situation, the pitcher's full arsenal, how both players have performed recently, and how this batter handles the types of pitches this pitcher throws.

**It cannot** account for:

- **Specific pitch sequences within the at-bat.** The model works at the PA level, not the pitch level. It doesn't know the current count.
- **Defensive positioning and quality.** Shifts and defensive metrics aren't yet in the feature set.
- **Game-day weather.** Wind, temperature, and humidity affect ball flight. Park factors partially capture average conditions.
- **Injuries or mechanical changes.** If a pitcher tweaked his delivery or a batter adjusted his stance, the model relies on historical data that doesn't reflect the change.

### Fallback Behavior

When a batter or pitcher doesn't have enough data (rookies, early season, September call-ups), the model falls back to **league-average profiles** for all features including recent form. This produces sensible baseline predictions rather than breaking. As the season progresses, predictions become more player-specific.

## What's Next

Future improvements we're exploring:

- **Catcher framing effects** -- the catcher behind the plate meaningfully shifts K/BB rates, and we already have the data
- **Umpire strike zone modeling** -- each umpire has a measurably different zone
- **Batter hot/cold zone maps** -- not just which pitch type, but where in the zone
- **Count-conditional predictions** -- updating probabilities as the count changes (0-2 vs 3-0)
- **Defensive quality integration** -- incorporating OAA and DRS to refine BABIP predictions

The matchup probability panel is live on all gamecast pages during games, and pregame predictions are available for every game with posted lineups. Check it out next time you're watching -- or betting.
