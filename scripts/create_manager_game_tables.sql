-- Manager Decision Game tables

CREATE TABLE IF NOT EXISTS manager_game_scenarios (
    id              SERIAL PRIMARY KEY,
    game_pk         INTEGER NOT NULL,
    game_date       DATE NOT NULL,
    away_team_abbr  VARCHAR(4),
    home_team_abbr  VARCHAR(4),
    inning          SMALLINT NOT NULL,
    half            VARCHAR(6) NOT NULL,
    outs            SMALLINT NOT NULL,
    away_score      SMALLINT NOT NULL,
    home_score      SMALLINT NOT NULL,
    base_state      SMALLINT NOT NULL,
    batter_id       INTEGER,
    batter_name     VARCHAR(100),
    pitcher_id      INTEGER,
    pitcher_name    VARCHAR(100),
    pitcher_pitch_count SMALLINT,
    pitcher_tto     SMALLINT,
    decision_type   VARCHAR(20) NOT NULL,
    actual_decision VARCHAR(3) NOT NULL,
    actual_detail   TEXT,
    engine_recommendation JSONB,
    context_json    JSONB NOT NULL,
    options_json    JSONB NOT NULL,
    play_index      INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mgr_scenarios_type ON manager_game_scenarios(decision_type);
CREATE INDEX IF NOT EXISTS idx_mgr_scenarios_date ON manager_game_scenarios(game_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mgr_scenarios_unique ON manager_game_scenarios(game_pk, play_index, decision_type);

CREATE TABLE IF NOT EXISTS manager_game_responses (
    id              SERIAL PRIMARY KEY,
    scenario_id     INTEGER NOT NULL REFERENCES manager_game_scenarios(id),
    session_uuid    VARCHAR(36) NOT NULL,
    user_choice     VARCHAR(3) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mgr_responses_scenario ON manager_game_responses(scenario_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mgr_responses_unique ON manager_game_responses(scenario_id, session_uuid);
