---
title: About Basenerd
---

<img src="/static/basenerd-logo-official.png" class="article-img-tiny">

# About Basenerd

Basenerd is a stats-first MLB analytics platform built for fans who want more than box scores. We combine real-time MLB data with proprietary machine learning models to deliver insights you can't find anywhere else.

## What We Do

**Live Scores & GameCast** — Every game, every pitch. Our gamecast tracks the action in real time with pitch-by-pitch data, live score updates, and play-by-play breakdowns.

**Matchup Predictions** — Our XGBoost machine learning model evaluates every batter-pitcher matchup and predicts the probability of every possible outcome: strikeout, home run, single, walk, and more. Predictions update live during games with Bayesian adjustments based on pitcher velocity and fatigue. [Read how the model works](/article/matchup-model-explained).

**Pregame Predictions** — Before first pitch, see projected outcomes for every batter in both lineups against the opposing starter. HR probability, K rate, hit rate, and hot/cold streaks — designed for fans who want to understand matchups and bettors looking for edges on player props.

**Player Profiles** — Deep-dive pages for every MLB player with year-by-year stats, career totals, awards, and accolades.

**Pitcher Reports** — Detailed pitching dashboards with arsenal breakdowns, pitch movement charts, release point analysis, location heatmaps, and our proprietary BNStuff+ and BNCtrl+ grades for every pitch type.

**Standings & Schedules** — Live standings for every division, team schedules, and transaction logs.

**Stat Leaderboards** — Sortable leaderboards across dozens of batting and pitching metrics.

**Team Pages** — 40-man rosters, schedules, transactions, and team-level analytics.

## The Analytics

Basenerd isn't just a stats aggregator. We build our own models:

- **BNStuff+** — Our proprietary pitch quality model that grades every pitch type on a 100-point scale based on velocity, movement, spin, and release characteristics
- **BNCtrl+** — Command and control grade measuring a pitcher's ability to locate pitches in the zone
- **Matchup Model** — 86-feature XGBoost classifier trained on 730,000+ plate appearances that predicts PA outcomes using batter profiles, pitcher arsenals, pitch-type matchups, park factors, game context, and rolling 14-day form
- **Pitch Selection Model** — Predicts what pitch a pitcher is likely to throw given the count, situation, and arsenal

All models are trained on Statcast pitch-level data from 2021-2025.

## The Tech

- **Backend:** Python / Flask
- **Data:** MLB StatsAPI + Statcast (PostgreSQL)
- **ML:** XGBoost, scikit-learn
- **Frontend:** Vanilla JS, server-rendered templates

Built by Nick Labella.

<img src="/static/x-logo-basenerd.png" class="article-img-icon"> [Follow Basenerd on X](https://www.x.com/basenerd_)
