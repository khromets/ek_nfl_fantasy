"""
Configuration settings for NFL data extraction
"""
import os
from pathlib import Path
from typing import Dict, List

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DEV_DIR = PROJECT_ROOT / "dev"
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Database settings
DATABASE_PATH = DATA_DIR / "nfl_fantasy.db"
SCHEMA_PATH = DEV_DIR / "database_schema.sql"

# API endpoints
ESPN_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
PRO_FOOTBALL_REFERENCE_BASE = "https://www.pro-football-reference.com"

# Data collection settings
SEASONS = [2020, 2021, 2022, 2023]  # Focus on completed seasons
CURRENT_SEASON = 2023

# Rate limiting (seconds between requests)
RATE_LIMITS = {
    'espn_api': 1.0,           # 1 second between ESPN API calls
    'pro_football_ref': 2.5,   # 2.5 seconds between PFR scraping requests
    'sleeper_api': 0.5,        # 0.5 seconds between Sleeper API calls
}

# Request settings
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BACKOFF_FACTOR = 2  # exponential backoff multiplier

# NFL team mappings
NFL_TEAMS = {
    'ARI': {'name': 'Arizona Cardinals', 'conference': 'NFC', 'division': 'West'},
    'ATL': {'name': 'Atlanta Falcons', 'conference': 'NFC', 'division': 'South'},
    'BAL': {'name': 'Baltimore Ravens', 'conference': 'AFC', 'division': 'North'},
    'BUF': {'name': 'Buffalo Bills', 'conference': 'AFC', 'division': 'East'},
    'CAR': {'name': 'Carolina Panthers', 'conference': 'NFC', 'division': 'South'},
    'CHI': {'name': 'Chicago Bears', 'conference': 'NFC', 'division': 'North'},
    'CIN': {'name': 'Cincinnati Bengals', 'conference': 'AFC', 'division': 'North'},
    'CLE': {'name': 'Cleveland Browns', 'conference': 'AFC', 'division': 'North'},
    'DAL': {'name': 'Dallas Cowboys', 'conference': 'NFC', 'division': 'East'},
    'DEN': {'name': 'Denver Broncos', 'conference': 'AFC', 'division': 'West'},
    'DET': {'name': 'Detroit Lions', 'conference': 'NFC', 'division': 'North'},
    'GB': {'name': 'Green Bay Packers', 'conference': 'NFC', 'division': 'North'},
    'HOU': {'name': 'Houston Texans', 'conference': 'AFC', 'division': 'South'},
    'IND': {'name': 'Indianapolis Colts', 'conference': 'AFC', 'division': 'South'},
    'JAX': {'name': 'Jacksonville Jaguars', 'conference': 'AFC', 'division': 'South'},
    'KC': {'name': 'Kansas City Chiefs', 'conference': 'AFC', 'division': 'West'},
    'LV': {'name': 'Las Vegas Raiders', 'conference': 'AFC', 'division': 'West'},
    'LAC': {'name': 'Los Angeles Chargers', 'conference': 'AFC', 'division': 'West'},
    'LAR': {'name': 'Los Angeles Rams', 'conference': 'NFC', 'division': 'West'},
    'MIA': {'name': 'Miami Dolphins', 'conference': 'AFC', 'division': 'East'},
    'MIN': {'name': 'Minnesota Vikings', 'conference': 'NFC', 'division': 'North'},
    'NE': {'name': 'New England Patriots', 'conference': 'AFC', 'division': 'East'},
    'NO': {'name': 'New Orleans Saints', 'conference': 'NFC', 'division': 'South'},
    'NYG': {'name': 'New York Giants', 'conference': 'NFC', 'division': 'East'},
    'NYJ': {'name': 'New York Jets', 'conference': 'AFC', 'division': 'East'},
    'PHI': {'name': 'Philadelphia Eagles', 'conference': 'NFC', 'division': 'East'},
    'PIT': {'name': 'Pittsburgh Steelers', 'conference': 'AFC', 'division': 'North'},
    'SF': {'name': 'San Francisco 49ers', 'conference': 'NFC', 'division': 'West'},
    'SEA': {'name': 'Seattle Seahawks', 'conference': 'NFC', 'division': 'West'},
    'TB': {'name': 'Tampa Bay Buccaneers', 'conference': 'NFC', 'division': 'South'},
    'TEN': {'name': 'Tennessee Titans', 'conference': 'AFC', 'division': 'South'},
    'WAS': {'name': 'Washington Commanders', 'conference': 'NFC', 'division': 'East'},
}

# Position mappings
OFFENSIVE_POSITIONS = ['QB', 'RB', 'WR', 'TE', 'K']
DEFENSIVE_POSITIONS = ['DT', 'DE', 'LB', 'CB', 'S', 'DL', 'DB']
ALL_POSITIONS = OFFENSIVE_POSITIONS + DEFENSIVE_POSITIONS

# Fantasy scoring rules (matching your league)
FANTASY_SCORING = {
    'passing_yards': 0.04,
    'passing_tds': 4.0,
    'interceptions_thrown': -2.0,
    'two_point_pass': 2.0,
    'rushing_yards': 0.1,
    'rushing_tds': 6.0,
    'two_point_rush': 2.0,
    'receiving_yards': 0.1,
    'receptions': 0.5,  # PPR
    'receiving_tds': 6.0,
    'two_point_reception': 2.0,
    'fumbles_lost': -2.0,
    'tackles_solo': 1.0,
    'tackles_assisted': 0.5,
    'sacks': 2.0,
    'interceptions': 2.0,
    'fumbles_forced': 2.0,
    'fumbles_recovered': 2.0,
    'passes_defended': 1.0,
    'safeties': 2.0,
    'defensive_tds': 6.0,
    'blocked_kicks': 2.0,
    'kick_return_tds': 6.0,
    'punt_return_tds': 6.0,
}

# Pro Football Reference URL patterns
PFR_URLS = {
    'team_roster': 'https://www.pro-football-reference.com/teams/{team}/{year}_roster.htm',
    'player_gamelog': 'https://www.pro-football-reference.com/players/{first_letter}/{player_id}/gamelog/{year}/',
    'weekly_stats': 'https://www.pro-football-reference.com/years/{year}/week_{week}.htm',
    'season_stats': 'https://www.pro-football-reference.com/years/{year}/opp.htm',
}

# Data validation thresholds
VALIDATION_THRESHOLDS = {
    'min_games_per_season': 250,        # Minimum games per NFL season
    'max_games_per_season': 285,        # Maximum games per NFL season (including playoffs)
    'min_players_per_team': 50,         # Minimum players per team roster
    'max_players_per_team': 90,         # Maximum players per team roster
    'min_passing_yards_game': 0,        # Minimum passing yards in a game
    'max_passing_yards_game': 600,      # Maximum reasonable passing yards
    'min_rushing_yards_game': -20,      # Minimum rushing yards (can be negative)
    'max_rushing_yards_game': 400,      # Maximum reasonable rushing yards
    'min_receiving_yards_game': 0,      # Minimum receiving yards
    'max_receiving_yards_game': 350,    # Maximum reasonable receiving yards
}

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': str(LOGS_DIR / 'nfl_data_extraction.log'),
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

# Environment-specific overrides
if os.getenv('NFL_DATA_ENV') == 'production':
    RATE_LIMITS['pro_football_ref'] = 5.0  # Slower in production
    MAX_RETRIES = 5

# Feature flags
ENABLE_CACHING = True
ENABLE_DATA_VALIDATION = True
ENABLE_PROGRESS_LOGGING = True