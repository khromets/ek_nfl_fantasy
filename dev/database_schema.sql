-- NFL Fantasy Football Database Schema
-- Database: SQLite/PostgreSQL compatible
-- Purpose: Store NFL player statistics and fantasy points for 2020-2024 seasons

-- =============================================================================
-- CORE TABLES
-- =============================================================================

-- Teams table - NFL team information
CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY,
    team_code VARCHAR(3) NOT NULL UNIQUE, -- 'KC', 'SF', etc.
    team_name VARCHAR(50) NOT NULL,       -- 'Kansas City Chiefs'
    conference VARCHAR(3) NOT NULL,       -- 'AFC' or 'NFC'
    division VARCHAR(10) NOT NULL,        -- 'North', 'South', 'East', 'West'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Players table - Master player roster
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    nfl_id VARCHAR(20) UNIQUE,           -- External NFL API ID
    name VARCHAR(100) NOT NULL,
    position VARCHAR(10) NOT NULL,        -- 'QB', 'RB', 'WR', 'TE', 'K', 'DEF', etc.
    team_id INTEGER,
    height_inches INTEGER,
    weight_lbs INTEGER,
    college VARCHAR(100),
    birth_date DATE,
    rookie_year INTEGER,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- Games table - Individual NFL games
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY,
    nfl_game_id VARCHAR(20) UNIQUE,      -- External NFL API game ID
    season INTEGER NOT NULL,              -- 2020, 2021, etc.
    week INTEGER NOT NULL,                -- 1-18 (regular season + playoffs)
    game_date DATE NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    game_type VARCHAR(20) DEFAULT 'REG',  -- 'REG', 'WC', 'DIV', 'CC', 'SB'
    weather_conditions TEXT,
    surface VARCHAR(20),                  -- 'Grass', 'Turf', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

-- Player game participation - tracks which players played in which games
CREATE TABLE IF NOT EXISTS player_games (
    player_game_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,            -- Player's team for this game (trades)
    active BOOLEAN DEFAULT TRUE,         -- Did player dress/participate
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    UNIQUE(player_id, game_id)
);

-- =============================================================================
-- OFFENSIVE STATISTICS TABLES
-- =============================================================================

-- Passing statistics (primarily QB)
CREATE TABLE IF NOT EXISTS passing_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    attempts INTEGER DEFAULT 0,
    completions INTEGER DEFAULT 0,
    passing_yards INTEGER DEFAULT 0,
    passing_tds INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    sacks INTEGER DEFAULT 0,
    sack_yards_lost INTEGER DEFAULT 0,
    longest_pass INTEGER DEFAULT 0,
    qb_rating DECIMAL(5,2),
    two_point_conversions INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- Rushing statistics (RB, QB, WR, TE)
CREATE TABLE IF NOT EXISTS rushing_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    attempts INTEGER DEFAULT 0,
    rushing_yards INTEGER DEFAULT 0,
    rushing_tds INTEGER DEFAULT 0,
    longest_rush INTEGER DEFAULT 0,
    fumbles INTEGER DEFAULT 0,
    fumbles_lost INTEGER DEFAULT 0,
    two_point_conversions INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- Receiving statistics (WR, TE, RB, QB)
CREATE TABLE IF NOT EXISTS receiving_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    targets INTEGER DEFAULT 0,
    receptions INTEGER DEFAULT 0,
    receiving_yards INTEGER DEFAULT 0,
    receiving_tds INTEGER DEFAULT 0,
    longest_reception INTEGER DEFAULT 0,
    fumbles INTEGER DEFAULT 0,
    fumbles_lost INTEGER DEFAULT 0,
    two_point_conversions INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- =============================================================================
-- DEFENSIVE STATISTICS TABLES
-- =============================================================================

-- Individual defensive player statistics
CREATE TABLE IF NOT EXISTS defensive_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    tackles_solo INTEGER DEFAULT 0,
    tackles_assisted INTEGER DEFAULT 0,
    tackles_total INTEGER DEFAULT 0,
    sacks DECIMAL(3,1) DEFAULT 0,         -- Can have half sacks
    sack_yards INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    int_return_yards INTEGER DEFAULT 0,
    int_return_tds INTEGER DEFAULT 0,
    passes_defended INTEGER DEFAULT 0,
    fumbles_forced INTEGER DEFAULT 0,
    fumbles_recovered INTEGER DEFAULT 0,
    fumble_return_yards INTEGER DEFAULT 0,
    fumble_return_tds INTEGER DEFAULT 0,
    safeties INTEGER DEFAULT 0,
    defensive_tds INTEGER DEFAULT 0,      -- All defensive TDs (INT, fumble, etc.)
    blocked_kicks INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- =============================================================================
-- SPECIAL TEAMS STATISTICS TABLES  
-- =============================================================================

-- Kicking statistics
CREATE TABLE IF NOT EXISTS kicking_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    field_goals_made INTEGER DEFAULT 0,
    field_goals_attempted INTEGER DEFAULT 0,
    fg_made_0_19 INTEGER DEFAULT 0,
    fg_made_20_29 INTEGER DEFAULT 0,
    fg_made_30_39 INTEGER DEFAULT 0,
    fg_made_40_49 INTEGER DEFAULT 0,
    fg_made_50_plus INTEGER DEFAULT 0,
    longest_fg INTEGER DEFAULT 0,
    extra_points_made INTEGER DEFAULT 0,
    extra_points_attempted INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- Punting statistics  
CREATE TABLE IF NOT EXISTS punting_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    punts INTEGER DEFAULT 0,
    punt_yards INTEGER DEFAULT 0,
    longest_punt INTEGER DEFAULT 0,
    punts_inside_20 INTEGER DEFAULT 0,
    punt_touchbacks INTEGER DEFAULT 0,
    punts_blocked INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- Return statistics (kick/punt returns)
CREATE TABLE IF NOT EXISTS return_stats (
    stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    kick_returns INTEGER DEFAULT 0,
    kick_return_yards INTEGER DEFAULT 0,
    kick_return_tds INTEGER DEFAULT 0,
    longest_kick_return INTEGER DEFAULT 0,
    punt_returns INTEGER DEFAULT 0,
    punt_return_yards INTEGER DEFAULT 0,
    punt_return_tds INTEGER DEFAULT 0,
    longest_punt_return INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- =============================================================================
-- FANTASY SCORING TABLES
-- =============================================================================

-- Fantasy scoring rules - configurable scoring system
CREATE TABLE IF NOT EXISTS scoring_rules (
    rule_id INTEGER PRIMARY KEY,
    stat_name VARCHAR(50) NOT NULL UNIQUE,
    points_per_unit DECIMAL(5,2) NOT NULL,
    description TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pre-populate with current league scoring rules
INSERT OR IGNORE INTO scoring_rules (stat_name, points_per_unit, description) VALUES
('passing_yards', 0.04, 'Points per passing yard'),
('passing_tds', 4.0, 'Points per passing touchdown'),
('interceptions_thrown', -2.0, 'Points lost per interception thrown'),
('two_point_pass', 2.0, 'Points per 2-point passing conversion'),
('rushing_yards', 0.1, 'Points per rushing yard'),
('rushing_tds', 6.0, 'Points per rushing touchdown'),
('two_point_rush', 2.0, 'Points per 2-point rushing conversion'),
('receiving_yards', 0.1, 'Points per receiving yard'),
('receptions', 0.5, 'Points per reception (PPR)'),
('receiving_tds', 6.0, 'Points per receiving touchdown'),
('two_point_reception', 2.0, 'Points per 2-point receiving conversion'),
('fumbles_lost', -2.0, 'Points lost per fumble lost'),
('tackles_solo', 1.0, 'Points per solo tackle'),
('tackles_assisted', 0.5, 'Points per assisted tackle'),
('sacks', 2.0, 'Points per sack'),
('interceptions', 2.0, 'Points per interception'),
('fumbles_forced', 2.0, 'Points per fumble forced'),
('fumbles_recovered', 2.0, 'Points per fumble recovered'),
('passes_defended', 1.0, 'Points per pass defended'),
('safeties', 2.0, 'Points per safety'),
('defensive_tds', 6.0, 'Points per defensive touchdown'),
('blocked_kicks', 2.0, 'Points per blocked kick'),
('kick_return_tds', 6.0, 'Points per kick return touchdown'),
('punt_return_tds', 6.0, 'Points per punt return touchdown');

-- Weekly fantasy points - calculated from raw stats
CREATE TABLE IF NOT EXISTS fantasy_points (
    fantasy_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    position VARCHAR(10) NOT NULL,
    
    -- Offensive points breakdown
    passing_points DECIMAL(6,2) DEFAULT 0,
    rushing_points DECIMAL(6,2) DEFAULT 0,
    receiving_points DECIMAL(6,2) DEFAULT 0,
    
    -- Defensive points breakdown  
    defensive_points DECIMAL(6,2) DEFAULT 0,
    special_teams_points DECIMAL(6,2) DEFAULT 0,
    
    -- Penalties
    fumble_points DECIMAL(6,2) DEFAULT 0,
    
    -- Total fantasy points for the game
    total_points DECIMAL(6,2) DEFAULT 0,
    
    calculation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    UNIQUE(player_id, game_id)
);

-- =============================================================================
-- AGGREGATED STATISTICS VIEWS/TABLES
-- =============================================================================

-- Season totals for players - materialized for performance
CREATE TABLE IF NOT EXISTS season_stats (
    season_stat_id INTEGER PRIMARY KEY,
    player_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    position VARCHAR(10) NOT NULL,
    games_played INTEGER DEFAULT 0,
    
    -- Offensive totals
    total_passing_yards INTEGER DEFAULT 0,
    total_passing_tds INTEGER DEFAULT 0,
    total_rushing_yards INTEGER DEFAULT 0,
    total_rushing_tds INTEGER DEFAULT 0,
    total_receiving_yards INTEGER DEFAULT 0,
    total_receptions INTEGER DEFAULT 0,
    total_receiving_tds INTEGER DEFAULT 0,
    
    -- Defensive totals
    total_tackles INTEGER DEFAULT 0,
    total_sacks DECIMAL(4,1) DEFAULT 0,
    total_interceptions INTEGER DEFAULT 0,
    
    -- Fantasy totals
    total_fantasy_points DECIMAL(8,2) DEFAULT 0,
    avg_fantasy_points DECIMAL(6,2) DEFAULT 0,
    
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    UNIQUE(player_id, season)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Core table indexes
CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team_id);
CREATE INDEX IF NOT EXISTS idx_players_active ON players(active);

CREATE INDEX IF NOT EXISTS idx_games_season_week ON games(season, week);
CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team_id, away_team_id);

-- Statistics table indexes
CREATE INDEX IF NOT EXISTS idx_passing_player_game ON passing_stats(player_id, game_id);
CREATE INDEX IF NOT EXISTS idx_rushing_player_game ON rushing_stats(player_id, game_id);
CREATE INDEX IF NOT EXISTS idx_receiving_player_game ON receiving_stats(player_id, game_id);
CREATE INDEX IF NOT EXISTS idx_defensive_player_game ON defensive_stats(player_id, game_id);

-- Fantasy points indexes
CREATE INDEX IF NOT EXISTS idx_fantasy_player_season ON fantasy_points(player_id, season);
CREATE INDEX IF NOT EXISTS idx_fantasy_position_season ON fantasy_points(position, season);
CREATE INDEX IF NOT EXISTS idx_fantasy_total_points ON fantasy_points(total_points DESC);

-- Season stats indexes  
CREATE INDEX IF NOT EXISTS idx_season_stats_position ON season_stats(position, season);
CREATE INDEX IF NOT EXISTS idx_season_stats_fantasy_points ON season_stats(total_fantasy_points DESC);