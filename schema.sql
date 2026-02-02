-- PWHL Database Schema
-- Professional Women's Hockey League Database
-- Seasons: 2023-24, 2024-25

-- Enable foreign key support
PRAGMA foreign_keys = ON;

-- =====================================================
-- CORE TABLES
-- =====================================================

-- Seasons table
CREATE TABLE seasons (
    season_id INTEGER PRIMARY KEY,
    season_name TEXT NOT NULL,
    shortname TEXT NOT NULL,
    career INTEGER NOT NULL,
    playoff INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

-- Teams table
CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    nickname TEXT NOT NULL,
    code TEXT NOT NULL UNIQUE,
    city TEXT NOT NULL,
    logo_url TEXT,
    league_id INTEGER DEFAULT 1,
    conference_id INTEGER DEFAULT 1,
    division_id INTEGER DEFAULT 1
);

-- Players table
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    position TEXT NOT NULL,  -- C, LW, RW, LD, RD, G
    position_analysis TEXT,  -- F, D, G (simplified)
    shoots TEXT,  -- L, R
    catches TEXT,  -- L, R (for goalies)
    height TEXT,
    weight INTEGER,
    birthdate DATE,
    hometown TEXT,
    home_province TEXT,
    home_country TEXT,
    nationality TEXT,
    player_image_url TEXT,
    active INTEGER DEFAULT 1
);

-- Roster assignments (player-team-season mapping)
CREATE TABLE roster_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    jersey_number INTEGER,
    rookie INTEGER DEFAULT 0,
    veteran INTEGER DEFAULT 0,
    status TEXT,  -- Signed, Camp Invitee, etc.
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    UNIQUE(player_id, team_id, season_id)
);

-- Games table
CREATE TABLE games (
    game_id INTEGER PRIMARY KEY,
    season_id INTEGER NOT NULL,
    game_number INTEGER,
    date_played DATETIME NOT NULL,
    timezone TEXT,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    home_goals INTEGER DEFAULT 0,
    away_goals INTEGER DEFAULT 0,
    periods INTEGER DEFAULT 3,
    overtime INTEGER DEFAULT 0,
    shootout INTEGER DEFAULT 0,
    game_status TEXT,  -- Final, Final OT, Final SO
    venue_name TEXT,
    venue_location TEXT,
    attendance INTEGER,
    home_coach TEXT,
    away_coach TEXT,
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

-- =====================================================
-- PLAYER STATISTICS TABLES
-- =====================================================

-- Skater season statistics
CREATE TABLE skater_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    
    -- Games
    games_played INTEGER DEFAULT 0,
    
    -- Scoring
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    points_per_game REAL DEFAULT 0,
    
    -- Shooting
    shots INTEGER DEFAULT 0,
    shooting_percentage REAL DEFAULT 0,
    
    -- Special Teams
    power_play_goals INTEGER DEFAULT 0,
    power_play_assists INTEGER DEFAULT 0,
    power_play_points INTEGER DEFAULT 0,
    short_handed_goals INTEGER DEFAULT 0,
    short_handed_assists INTEGER DEFAULT 0,
    short_handed_points INTEGER DEFAULT 0,
    
    -- Shootout
    shootout_goals INTEGER DEFAULT 0,
    shootout_attempts INTEGER DEFAULT 0,
    shootout_percentage REAL DEFAULT 0,
    shootout_games_played INTEGER DEFAULT 0,
    
    -- Special Goals
    game_winning_goals INTEGER DEFAULT 0,
    first_goals INTEGER DEFAULT 0,
    insurance_goals INTEGER DEFAULT 0,
    empty_net_goals INTEGER DEFAULT 0,
    overtime_goals INTEGER DEFAULT 0,
    unassisted_goals INTEGER DEFAULT 0,
    
    -- Penalties
    penalty_minutes INTEGER DEFAULT 0,
    penalty_minutes_per_game REAL DEFAULT 0,
    minor_penalties INTEGER DEFAULT 0,
    major_penalties INTEGER DEFAULT 0,
    
    -- Physical/Defense
    hits INTEGER DEFAULT 0,
    hits_per_game REAL DEFAULT 0,
    shots_blocked_by_player INTEGER DEFAULT 0,
    plus_minus INTEGER DEFAULT 0,
    
    -- Faceoffs
    faceoff_attempts INTEGER DEFAULT 0,
    faceoff_wins INTEGER DEFAULT 0,
    faceoff_percentage REAL DEFAULT 0,
    
    -- Ice Time
    ice_time_seconds INTEGER DEFAULT 0,
    ice_time_avg_seconds INTEGER DEFAULT 0,
    ice_time_per_game_avg TEXT,
    
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    UNIQUE(player_id, team_id, season_id)
);

-- Goalie season statistics
CREATE TABLE goalie_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    
    -- Games
    games_played INTEGER DEFAULT 0,
    
    -- Playing Time
    minutes_played INTEGER DEFAULT 0,
    seconds_played INTEGER DEFAULT 0,
    
    -- Wins/Losses
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    ot_losses INTEGER DEFAULT 0,
    total_losses INTEGER DEFAULT 0,
    shutouts INTEGER DEFAULT 0,
    
    -- Goaltending
    saves INTEGER DEFAULT 0,
    shots_against INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    empty_net_goals_against INTEGER DEFAULT 0,
    save_percentage REAL DEFAULT 0,
    goals_against_average REAL DEFAULT 0,
    shots_against_average REAL DEFAULT 0,
    
    -- Shootout
    shootout_games_played INTEGER DEFAULT 0,
    shootout_losses INTEGER DEFAULT 0,
    shootout_wins INTEGER DEFAULT 0,
    shootout_goals_against INTEGER DEFAULT 0,
    shootout_saves INTEGER DEFAULT 0,
    shootout_attempts INTEGER DEFAULT 0,
    shootout_percentage REAL DEFAULT 0,
    
    -- Offense (goalies sometimes score/assist)
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    penalty_minutes INTEGER DEFAULT 0,
    
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    UNIQUE(player_id, team_id, season_id)
);

-- =====================================================
-- TEAM STATISTICS TABLES
-- =====================================================

-- Team season statistics/standings
CREATE TABLE team_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    
    -- Record
    games_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    ot_losses INTEGER DEFAULT 0,
    shootout_wins INTEGER DEFAULT 0,
    shootout_losses INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    
    -- Win Percentages
    win_percentage REAL DEFAULT 0,
    percentage REAL DEFAULT 0,
    
    -- Goals
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    goals_diff INTEGER DEFAULT 0,
    
    -- Special Teams
    power_plays INTEGER DEFAULT 0,
    power_play_goals INTEGER DEFAULT 0,
    power_play_percentage REAL DEFAULT 0,
    times_short_handed INTEGER DEFAULT 0,
    power_play_goals_against INTEGER DEFAULT 0,
    penalty_kill_percentage REAL DEFAULT 0,
    short_handed_goals_for INTEGER DEFAULT 0,
    short_handed_goals_against INTEGER DEFAULT 0,
    
    -- Shootout
    shootout_goals INTEGER DEFAULT 0,
    shootout_goals_against INTEGER DEFAULT 0,
    shootout_attempts INTEGER DEFAULT 0,
    shootout_attempts_against INTEGER DEFAULT 0,
    shootout_games_played INTEGER DEFAULT 0,
    shootout_percentage REAL DEFAULT 0,
    
    -- Penalties
    penalty_minutes INTEGER DEFAULT 0,
    pim_per_game REAL DEFAULT 0,
    
    -- Streaks
    streak TEXT,
    past_10 TEXT,
    home_record TEXT,
    away_record TEXT,
    shootout_record TEXT,
    
    -- Rankings
    overall_rank INTEGER DEFAULT 0,
    division_rank INTEGER DEFAULT 0,
    clinched_playoff_spot INTEGER DEFAULT 0,
    
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    UNIQUE(team_id, season_id)
);

-- =====================================================
-- PLAY-BY-PLAY TABLES
-- =====================================================

-- Shots table (for shot tracking and potential Corsi/Fenwick)
CREATE TABLE shots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    game_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    goalie_id INTEGER,
    team_id INTEGER NOT NULL,
    opponent_team_id INTEGER NOT NULL,
    is_home INTEGER,
    period INTEGER,
    time_formatted TEXT,
    seconds INTEGER,
    x_location INTEGER,
    y_location INTEGER,
    shot_type TEXT,
    shot_type_description TEXT,
    quality INTEGER,
    shot_quality_description TEXT,
    is_goal INTEGER DEFAULT 0,
    game_goal_id INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (goalie_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (opponent_team_id) REFERENCES teams(team_id)
);

-- Goals table
CREATE TABLE goals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    game_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    goal_id INTEGER,
    team_id INTEGER NOT NULL,
    scorer_id INTEGER NOT NULL,
    assist1_id INTEGER,
    assist2_id INTEGER,
    goalie_id INTEGER,
    opponent_team_id INTEGER NOT NULL,
    is_home INTEGER,
    period INTEGER,
    time_formatted TEXT,
    seconds INTEGER,
    x_location INTEGER,
    y_location INTEGER,
    goal_type TEXT,
    power_play INTEGER DEFAULT 0,
    short_handed INTEGER DEFAULT 0,
    empty_net INTEGER DEFAULT 0,
    game_winning INTEGER DEFAULT 0,
    insurance_goal INTEGER DEFAULT 0,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (scorer_id) REFERENCES players(player_id),
    FOREIGN KEY (assist1_id) REFERENCES players(player_id),
    FOREIGN KEY (assist2_id) REFERENCES players(player_id),
    FOREIGN KEY (goalie_id) REFERENCES players(player_id),
    FOREIGN KEY (opponent_team_id) REFERENCES teams(team_id)
);

-- Penalties table
CREATE TABLE penalties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    game_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    is_home INTEGER,
    period INTEGER,
    time_formatted TEXT,
    seconds INTEGER,  -- nullable as not all sources have this
    minutes INTEGER,
    penalty_class TEXT,
    penalty_description TEXT,
    is_bench INTEGER DEFAULT 0,
    is_penalty_shot INTEGER DEFAULT 0,
    pp INTEGER DEFAULT 0,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- Faceoffs table
CREATE TABLE faceoffs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    game_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    home_player_id INTEGER,
    away_player_id INTEGER,
    home_team_id INTEGER,
    away_team_id INTEGER,
    period INTEGER,
    time_formatted TEXT,
    seconds INTEGER,
    x_location INTEGER,
    y_location INTEGER,
    location_id INTEGER,
    home_win INTEGER,
    win_team_id INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (home_player_id) REFERENCES players(player_id),
    FOREIGN KEY (away_player_id) REFERENCES players(player_id),
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

-- Hits table
CREATE TABLE hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    game_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    is_home INTEGER,
    period INTEGER,
    time_formatted TEXT,
    seconds INTEGER,
    x_location INTEGER,
    y_location INTEGER,
    hit_type INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- Blocked shots table
CREATE TABLE blocked_shots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    game_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    blocker_id INTEGER,
    shooter_id INTEGER,
    team_id INTEGER NOT NULL,  -- blocking team
    opponent_team_id INTEGER NOT NULL,
    is_home INTEGER,
    period INTEGER,
    time_formatted TEXT,
    seconds INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    FOREIGN KEY (blocker_id) REFERENCES players(player_id),
    FOREIGN KEY (shooter_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (opponent_team_id) REFERENCES teams(team_id)
);

-- =====================================================
-- GAME PLAYER STATS (Game-by-game statistics)
-- =====================================================

CREATE TABLE game_player_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    season_id INTEGER NOT NULL,
    
    -- Position played
    position TEXT,
    
    -- Basic stats
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    plus_minus INTEGER DEFAULT 0,
    penalty_minutes INTEGER DEFAULT 0,
    
    -- Ice time (if available)
    ice_time_seconds INTEGER,
    shifts INTEGER,
    
    -- Faceoffs
    faceoff_wins INTEGER DEFAULT 0,
    faceoff_losses INTEGER DEFAULT 0,
    
    -- Hits/Blocks
    hits INTEGER DEFAULT 0,
    blocked_shots INTEGER DEFAULT 0,
    
    -- Power play
    power_play_goals INTEGER DEFAULT 0,
    power_play_assists INTEGER DEFAULT 0,
    
    -- Short handed
    short_handed_goals INTEGER DEFAULT 0,
    short_handed_assists INTEGER DEFAULT 0,
    
    -- Special
    game_winning_goal INTEGER DEFAULT 0,
    first_goal INTEGER DEFAULT 0,
    empty_net_goal INTEGER DEFAULT 0,
    overtime_goal INTEGER DEFAULT 0,
    
    -- Goalie stats (if applicable)
    saves INTEGER,
    shots_against INTEGER,
    goals_against INTEGER,
    save_percentage REAL,
    
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id),
    UNIQUE(game_id, player_id)
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

CREATE INDEX idx_games_season ON games(season_id);
CREATE INDEX idx_games_date ON games(date_played);
CREATE INDEX idx_games_home_team ON games(home_team_id);
CREATE INDEX idx_games_away_team ON games(away_team_id);

CREATE INDEX idx_skater_stats_player ON skater_stats(player_id);
CREATE INDEX idx_skater_stats_season ON skater_stats(season_id);
CREATE INDEX idx_skater_stats_team ON skater_stats(team_id);

CREATE INDEX idx_goalie_stats_player ON goalie_stats(player_id);
CREATE INDEX idx_goalie_stats_season ON goalie_stats(season_id);
CREATE INDEX idx_goalie_stats_team ON goalie_stats(team_id);

CREATE INDEX idx_team_stats_season ON team_stats(season_id);
CREATE INDEX idx_team_stats_team ON team_stats(team_id);

CREATE INDEX idx_shots_game ON shots(game_id);
CREATE INDEX idx_shots_player ON shots(player_id);
CREATE INDEX idx_shots_season ON shots(season_id);

CREATE INDEX idx_goals_game ON goals(game_id);
CREATE INDEX idx_goals_scorer ON goals(scorer_id);
CREATE INDEX idx_goals_season ON goals(season_id);

CREATE INDEX idx_penalties_game ON penalties(game_id);
CREATE INDEX idx_penalties_player ON penalties(player_id);
CREATE INDEX idx_penalties_season ON penalties(season_id);

CREATE INDEX idx_faceoffs_game ON faceoffs(game_id);
CREATE INDEX idx_faceoffs_season ON faceoffs(season_id);

CREATE INDEX idx_hits_game ON hits(game_id);
CREATE INDEX idx_hits_player ON hits(player_id);

CREATE INDEX idx_blocked_shots_game ON blocked_shots(game_id);
CREATE INDEX idx_game_player_stats_game ON game_player_stats(game_id);
CREATE INDEX idx_game_player_stats_player ON game_player_stats(player_id);

CREATE INDEX idx_roster_assignments_player ON roster_assignments(player_id);
CREATE INDEX idx_roster_assignments_team ON roster_assignments(team_id);
CREATE INDEX idx_roster_assignments_season ON roster_assignments(season_id);
