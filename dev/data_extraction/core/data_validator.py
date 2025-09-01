"""
Data validation utilities for NFL fantasy data
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from datetime import datetime, date
from .config import VALIDATION_THRESHOLDS, NFL_TEAMS, SEASONS

class DataValidator:
    """
    Validates extracted NFL data for completeness and accuracy
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_errors = []
        self.validation_warnings = []
    
    def validate_team_data(self, teams: List[Dict[str, Any]]) -> bool:
        """
        Validate team data
        
        Args:
            teams: List of team dictionaries
            
        Returns:
            True if valid, False otherwise
        """
        is_valid = True
        
        # Check team count
        if len(teams) != 32:
            self.validation_errors.append(f"Expected 32 NFL teams, got {len(teams)}")
            is_valid = False
        
        # Check required fields
        required_fields = ['team_code', 'team_name', 'conference', 'division']
        for team in teams:
            for field in required_fields:
                if field not in team or not team[field]:
                    self.validation_errors.append(f"Team missing required field '{field}': {team}")
                    is_valid = False
        
        # Check team codes match expected
        team_codes = {team.get('team_code') for team in teams if 'team_code' in team}
        expected_codes = set(NFL_TEAMS.keys())
        
        missing_codes = expected_codes - team_codes
        extra_codes = team_codes - expected_codes
        
        if missing_codes:
            self.validation_errors.append(f"Missing expected team codes: {missing_codes}")
            is_valid = False
        
        if extra_codes:
            self.validation_warnings.append(f"Unexpected team codes: {extra_codes}")
        
        # Check conferences and divisions
        conferences = {team.get('conference') for team in teams if 'conference' in team}
        expected_conferences = {'AFC', 'NFC'}
        
        if not conferences.issubset(expected_conferences):
            self.validation_errors.append(f"Invalid conferences found: {conferences - expected_conferences}")
            is_valid = False
        
        return is_valid
    
    def validate_player_data(self, players: List[Dict[str, Any]]) -> bool:
        """
        Validate player data
        
        Args:
            players: List of player dictionaries
            
        Returns:
            True if valid, False otherwise
        """
        is_valid = True
        
        if not players:
            self.validation_errors.append("No player data provided")
            return False
        
        # Check required fields
        required_fields = ['name', 'position']
        for player in players:
            for field in required_fields:
                if field not in player or not player[field]:
                    self.validation_errors.append(f"Player missing required field '{field}': {player}")
                    is_valid = False
        
        # Check position validity
        valid_positions = {'QB', 'RB', 'WR', 'TE', 'K', 'DEF', 'DT', 'DE', 'LB', 'CB', 'S', 'DL', 'DB', 'P', 'LS'}
        positions = {player.get('position') for player in players if 'position' in player}
        invalid_positions = positions - valid_positions
        
        if invalid_positions:
            self.validation_warnings.append(f"Unexpected positions found: {invalid_positions}")
        
        # Check for duplicate players (same name + position + team)
        player_keys = []
        for player in players:
            key = (player.get('name', ''), player.get('position', ''), player.get('team_id', ''))
            if key in player_keys:
                self.validation_warnings.append(f"Duplicate player found: {player}")
            player_keys.append(key)
        
        return is_valid
    
    def validate_game_data(self, games: List[Dict[str, Any]], season: int) -> bool:
        """
        Validate game data for a season
        
        Args:
            games: List of game dictionaries
            season: Season year
            
        Returns:
            True if valid, False otherwise
        """
        is_valid = True
        
        if not games:
            self.validation_errors.append(f"No game data provided for season {season}")
            return False
        
        # Check game count (regular season + playoffs)
        game_count = len(games)
        min_games = VALIDATION_THRESHOLDS['min_games_per_season']
        max_games = VALIDATION_THRESHOLDS['max_games_per_season']
        
        if game_count < min_games or game_count > max_games:
            self.validation_warnings.append(
                f"Season {season} has {game_count} games, expected {min_games}-{max_games}"
            )
        
        # Check required fields
        required_fields = ['season', 'week', 'game_date', 'home_team_id', 'away_team_id']
        for game in games:
            for field in required_fields:
                if field not in game or game[field] is None:
                    self.validation_errors.append(f"Game missing required field '{field}': {game}")
                    is_valid = False
        
        # Check season consistency
        seasons_in_data = {game.get('season') for game in games if 'season' in game}
        if len(seasons_in_data) > 1:
            self.validation_errors.append(f"Multiple seasons in game data: {seasons_in_data}")
            is_valid = False
        
        # Check week numbers
        weeks = {game.get('week') for game in games if 'week' in game and game['week'] is not None}
        if weeks and (min(weeks) < 1 or max(weeks) > 22):  # Including playoffs
            self.validation_warnings.append(f"Unusual week numbers found: {sorted(weeks)}")
        
        # Check dates are in correct year
        for game in games:
            if 'game_date' in game and game['game_date']:
                try:
                    if isinstance(game['game_date'], str):
                        game_date = datetime.strptime(game['game_date'], '%Y-%m-%d').date()
                    else:
                        game_date = game['game_date']
                    
                    if game_date.year not in [season, season + 1]:  # Playoffs can be in following year
                        self.validation_warnings.append(
                            f"Game date {game_date} seems wrong for season {season}"
                        )
                except Exception as e:
                    self.validation_errors.append(f"Invalid game date format: {game['game_date']}")
                    is_valid = False
        
        return is_valid
    
    def validate_stats_data(self, stats: List[Dict[str, Any]], stat_type: str) -> bool:
        """
        Validate statistical data
        
        Args:
            stats: List of statistics dictionaries
            stat_type: Type of stats ('passing', 'rushing', 'receiving', etc.)
            
        Returns:
            True if valid, False otherwise
        """
        is_valid = True
        
        if not stats:
            self.validation_warnings.append(f"No {stat_type} stats provided")
            return True  # Empty stats might be valid
        
        # Check required fields based on stat type
        required_fields = {
            'passing': ['player_id', 'game_id', 'attempts', 'completions', 'passing_yards'],
            'rushing': ['player_id', 'game_id', 'attempts', 'rushing_yards'],
            'receiving': ['player_id', 'game_id', 'targets', 'receptions', 'receiving_yards'],
            'defensive': ['player_id', 'game_id', 'tackles_total'],
            'kicking': ['player_id', 'game_id', 'field_goals_attempted'],
        }
        
        fields_to_check = required_fields.get(stat_type, ['player_id', 'game_id'])
        
        for stat in stats:
            for field in fields_to_check:
                if field not in stat:
                    self.validation_errors.append(f"{stat_type} stat missing field '{field}': {stat}")
                    is_valid = False
        
        # Validate statistical ranges
        if stat_type == 'passing':
            for stat in stats:
                yards = stat.get('passing_yards', 0)
                if yards < 0 or yards > VALIDATION_THRESHOLDS['max_passing_yards_game']:
                    self.validation_warnings.append(f"Unusual passing yards: {yards} in {stat}")
                
                completions = stat.get('completions', 0)
                attempts = stat.get('attempts', 0)
                if completions > attempts:
                    self.validation_errors.append(f"Completions > attempts in {stat}")
                    is_valid = False
        
        elif stat_type == 'rushing':
            for stat in stats:
                yards = stat.get('rushing_yards', 0)
                min_yards = VALIDATION_THRESHOLDS['min_rushing_yards_game']
                max_yards = VALIDATION_THRESHOLDS['max_rushing_yards_game']
                if yards < min_yards or yards > max_yards:
                    self.validation_warnings.append(f"Unusual rushing yards: {yards} in {stat}")
        
        elif stat_type == 'receiving':
            for stat in stats:
                yards = stat.get('receiving_yards', 0)
                min_yards = VALIDATION_THRESHOLDS['min_receiving_yards_game']
                max_yards = VALIDATION_THRESHOLDS['max_receiving_yards_game']
                if yards < min_yards or yards > max_yards:
                    self.validation_warnings.append(f"Unusual receiving yards: {yards} in {stat}")
                
                receptions = stat.get('receptions', 0)
                targets = stat.get('targets', 0)
                if targets > 0 and receptions > targets:
                    self.validation_errors.append(f"Receptions > targets in {stat}")
                    is_valid = False
        
        return is_valid
    
    def validate_fantasy_points(self, fantasy_points: List[Dict[str, Any]]) -> bool:
        """
        Validate fantasy points calculations
        
        Args:
            fantasy_points: List of fantasy point dictionaries
            
        Returns:
            True if valid, False otherwise
        """
        is_valid = True
        
        if not fantasy_points:
            self.validation_warnings.append("No fantasy points data provided")
            return True
        
        required_fields = ['player_id', 'game_id', 'total_points']
        for fp in fantasy_points:
            for field in required_fields:
                if field not in fp:
                    self.validation_errors.append(f"Fantasy points missing field '{field}': {fp}")
                    is_valid = False
        
        # Check for reasonable point ranges
        for fp in fantasy_points:
            total = fp.get('total_points', 0)
            if total < -10 or total > 60:  # Very loose bounds
                self.validation_warnings.append(f"Unusual fantasy points: {total} in {fp}")
        
        return is_valid
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get summary of validation results
        
        Returns:
            Dictionary with validation summary
        """
        return {
            'errors': self.validation_errors,
            'warnings': self.validation_warnings,
            'error_count': len(self.validation_errors),
            'warning_count': len(self.validation_warnings),
            'is_valid': len(self.validation_errors) == 0
        }
    
    def reset_validation_state(self):
        """Reset validation errors and warnings"""
        self.validation_errors = []
        self.validation_warnings = []
    
    def log_validation_results(self):
        """Log validation results"""
        summary = self.get_validation_summary()
        
        if summary['error_count'] > 0:
            self.logger.error(f"Data validation failed with {summary['error_count']} errors:")
            for error in summary['errors']:
                self.logger.error(f"  ERROR: {error}")
        
        if summary['warning_count'] > 0:
            self.logger.warning(f"Data validation completed with {summary['warning_count']} warnings:")
            for warning in summary['warnings']:
                self.logger.warning(f"  WARNING: {warning}")
        
        if summary['is_valid'] and summary['warning_count'] == 0:
            self.logger.info("Data validation passed successfully")


def validate_data_completeness(db_manager, seasons: List[int] = None) -> Dict[str, Any]:
    """
    Validate completeness of data in database
    
    Args:
        db_manager: DatabaseManager instance
        seasons: List of seasons to validate
        
    Returns:
        Dictionary with completeness report
    """
    if seasons is None:
        seasons = SEASONS
    
    logger = logging.getLogger(__name__)
    report = {}
    
    try:
        # Check teams
        teams_count = db_manager.get_table_count('teams')
        report['teams'] = {
            'count': teams_count,
            'expected': 32,
            'complete': teams_count == 32
        }
        
        # Check games by season
        report['games'] = {}
        for season in seasons:
            games_sql = "SELECT COUNT(*) as count FROM games WHERE season = ?"
            result = db_manager.query(games_sql, (season,))
            count = result[0]['count'] if result else 0
            
            report['games'][season] = {
                'count': count,
                'expected_min': VALIDATION_THRESHOLDS['min_games_per_season'],
                'expected_max': VALIDATION_THRESHOLDS['max_games_per_season'],
                'complete': (VALIDATION_THRESHOLDS['min_games_per_season'] <= 
                           count <= VALIDATION_THRESHOLDS['max_games_per_season'])
            }
        
        # Check players
        players_count = db_manager.get_table_count('players')
        report['players'] = {
            'count': players_count,
            'expected_min': 32 * VALIDATION_THRESHOLDS['min_players_per_team'],
            'expected_max': 32 * VALIDATION_THRESHOLDS['max_players_per_team']
        }
        
        # Check statistics tables
        stat_tables = ['passing_stats', 'rushing_stats', 'receiving_stats', 'defensive_stats']
        report['statistics'] = {}
        
        for table in stat_tables:
            count = db_manager.get_table_count(table)
            report['statistics'][table] = {'count': count}
        
        logger.info("Data completeness validation completed")
        
    except Exception as e:
        logger.error(f"Failed to validate data completeness: {e}")
        report['error'] = str(e)
    
    return report