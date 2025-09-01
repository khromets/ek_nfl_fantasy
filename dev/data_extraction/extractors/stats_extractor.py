"""
Statistical data extraction from ESPN API
"""
import requests
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from ..core.config import ESPN_BASE_URL, REQUEST_HEADERS, RATE_LIMITS
from ..core.rate_limiter import get_adaptive_rate_limiter
from ..core.data_validator import DataValidator

class StatsExtractor:
    """
    Extract player statistics from ESPN API
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = get_adaptive_rate_limiter()
        self.validator = DataValidator()
        self.team_mappings = {}  # team_code -> team_id
        self.player_mappings = {}  # (name, position) -> player_id
    
    def set_mappings(self, team_mappings: Dict[str, int], player_mappings: Dict[tuple, int] = None):
        """Set team and player mappings"""
        self.team_mappings = team_mappings
        if player_mappings:
            self.player_mappings = player_mappings
        self.logger.info(f"Set mappings: {len(team_mappings)} teams, {len(self.player_mappings)} players")
    
    def extract_game_stats(self, game_id: str, season: int, week: int) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract player statistics for a specific game from ESPN API
        
        Args:
            game_id: ESPN game ID
            season: Season year
            week: Week number
            
        Returns:
            Dictionary with stats by category
        """
        stats = {
            'passing': [],
            'rushing': [],
            'receiving': [],
            'defensive': []
        }
        
        try:
            self.rate_limiter.wait_for_request('espn_api')
            
            # ESPN game API endpoint
            url = f"{ESPN_BASE_URL}/summary"
            params = {'event': game_id}
            
            response = requests.get(url, headers=REQUEST_HEADERS, params=params, timeout=30)
            success = response.status_code == 200
            self.rate_limiter.wait_for_request('espn_api', success, response.status_code)
            
            if not success:
                self.logger.warning(f"Failed to get game stats for {game_id}: HTTP {response.status_code}")
                return stats
            
            data = response.json()
            
            # Extract boxscore data
            if 'boxscore' in data:
                boxscore = data['boxscore']
                
                # Process team stats
                if 'teams' in boxscore:
                    for team_data in boxscore['teams']:
                        team_stats = self._process_team_boxscore(team_data, game_id, season, week)
                        
                        # Merge stats by category
                        for category, category_stats in team_stats.items():
                            stats[category].extend(category_stats)
            
            # Extract play-by-play for additional stats if available
            if 'drives' in data:
                additional_stats = self._extract_from_drives(data['drives'], game_id, season, week)
                
                # Merge additional stats
                for category, category_stats in additional_stats.items():
                    stats[category].extend(category_stats)
            
            self.logger.debug(f"Extracted stats for game {game_id}: {len(stats['passing'])} passing, "
                            f"{len(stats['rushing'])} rushing, {len(stats['receiving'])} receiving")
            
        except Exception as e:
            self.logger.error(f"Failed to extract stats for game {game_id}: {e}")
        
        return stats
    
    def _process_team_boxscore(self, team_data: Dict[str, Any], game_id: str, 
                              season: int, week: int) -> Dict[str, List[Dict[str, Any]]]:
        """Process team boxscore data"""
        stats = {
            'passing': [],
            'rushing': [],
            'receiving': [],
            'defensive': []
        }
        
        try:
            team_info = team_data.get('team', {})
            team_abbrev = team_info.get('abbreviation', '').upper()
            
            # Map ESPN team code to our standard
            if team_abbrev == 'WSH':
                team_abbrev = 'WAS'
            
            team_id = self.team_mappings.get(team_abbrev)
            if not team_id:
                self.logger.warning(f"No team ID found for {team_abbrev}")
                return stats
            
            # Process statistics by category
            if 'statistics' in team_data:
                for stat_category in team_data['statistics']:
                    category_name = stat_category.get('name', '').lower()
                    
                    if 'passing' in category_name:
                        stats['passing'].extend(
                            self._process_passing_stats(stat_category, team_id, game_id, season, week)
                        )
                    elif 'rushing' in category_name:
                        stats['rushing'].extend(
                            self._process_rushing_stats(stat_category, team_id, game_id, season, week)
                        )
                    elif 'receiving' in category_name:
                        stats['receiving'].extend(
                            self._process_receiving_stats(stat_category, team_id, game_id, season, week)
                        )
                    elif 'defensive' in category_name or 'defense' in category_name:
                        stats['defensive'].extend(
                            self._process_defensive_stats(stat_category, team_id, game_id, season, week)
                        )
        
        except Exception as e:
            self.logger.error(f"Failed to process team boxscore: {e}")
        
        return stats
    
    def _process_passing_stats(self, stat_category: Dict[str, Any], team_id: int,
                              game_id: str, season: int, week: int) -> List[Dict[str, Any]]:
        """Process passing statistics"""
        passing_stats = []
        
        try:
            if 'athletes' not in stat_category:
                return passing_stats
            
            for athlete in stat_category['athletes']:
                athlete_info = athlete.get('athlete', {})
                name = athlete_info.get('displayName', '')
                position = athlete_info.get('position', {}).get('abbreviation', '')
                
                if not name or position != 'QB':
                    continue
                
                # Get or create player
                player_id = self._get_or_create_player(name, position, team_id)
                if not player_id:
                    continue
                
                # Extract stats
                stats = athlete.get('stats', [])
                stat_values = {stat.split('-')[0].strip(): int(stat.split('-')[1].strip()) 
                              for stat in stats if '-' in stat and stat.split('-')[1].strip().isdigit()}
                
                passing_stat = {
                    'player_id': player_id,
                    'game_id': game_id,
                    'season': season,
                    'week': week,
                    'attempts': stat_values.get('ATT', 0),
                    'completions': stat_values.get('CMP', 0),
                    'passing_yards': stat_values.get('YDS', 0),
                    'passing_tds': stat_values.get('TD', 0),
                    'interceptions': stat_values.get('INT', 0),
                    'sacks': stat_values.get('SACK', 0),
                    'two_point_conversions': 0  # Not typically in basic stats
                }
                
                passing_stats.append(passing_stat)
        
        except Exception as e:
            self.logger.error(f"Failed to process passing stats: {e}")
        
        return passing_stats
    
    def _process_rushing_stats(self, stat_category: Dict[str, Any], team_id: int,
                              game_id: str, season: int, week: int) -> List[Dict[str, Any]]:
        """Process rushing statistics"""
        rushing_stats = []
        
        try:
            if 'athletes' not in stat_category:
                return rushing_stats
            
            for athlete in stat_category['athletes']:
                athlete_info = athlete.get('athlete', {})
                name = athlete_info.get('displayName', '')
                position = athlete_info.get('position', {}).get('abbreviation', '')
                
                if not name:
                    continue
                
                player_id = self._get_or_create_player(name, position, team_id)
                if not player_id:
                    continue
                
                # Extract stats
                stats = athlete.get('stats', [])
                stat_values = {stat.split('-')[0].strip(): int(stat.split('-')[1].strip()) 
                              for stat in stats if '-' in stat and stat.split('-')[1].strip().isdigit()}
                
                rushing_stat = {
                    'player_id': player_id,
                    'game_id': game_id,
                    'season': season,
                    'week': week,
                    'attempts': stat_values.get('ATT', 0),
                    'rushing_yards': stat_values.get('YDS', 0),
                    'rushing_tds': stat_values.get('TD', 0),
                    'fumbles': stat_values.get('FUM', 0),
                    'fumbles_lost': 0,  # Not in basic stats
                    'two_point_conversions': 0
                }
                
                rushing_stats.append(rushing_stat)
        
        except Exception as e:
            self.logger.error(f"Failed to process rushing stats: {e}")
        
        return rushing_stats
    
    def _process_receiving_stats(self, stat_category: Dict[str, Any], team_id: int,
                                game_id: str, season: int, week: int) -> List[Dict[str, Any]]:
        """Process receiving statistics"""
        receiving_stats = []
        
        try:
            if 'athletes' not in stat_category:
                return receiving_stats
            
            for athlete in stat_category['athletes']:
                athlete_info = athlete.get('athlete', {})
                name = athlete_info.get('displayName', '')
                position = athlete_info.get('position', {}).get('abbreviation', '')
                
                if not name:
                    continue
                
                player_id = self._get_or_create_player(name, position, team_id)
                if not player_id:
                    continue
                
                # Extract stats
                stats = athlete.get('stats', [])
                stat_values = {stat.split('-')[0].strip(): int(stat.split('-')[1].strip()) 
                              for stat in stats if '-' in stat and stat.split('-')[1].strip().isdigit()}
                
                receiving_stat = {
                    'player_id': player_id,
                    'game_id': game_id,
                    'season': season,
                    'week': week,
                    'targets': stat_values.get('TAR', 0),
                    'receptions': stat_values.get('REC', 0),
                    'receiving_yards': stat_values.get('YDS', 0),
                    'receiving_tds': stat_values.get('TD', 0),
                    'fumbles': stat_values.get('FUM', 0),
                    'fumbles_lost': 0,
                    'two_point_conversions': 0
                }
                
                receiving_stats.append(receiving_stat)
        
        except Exception as e:
            self.logger.error(f"Failed to process receiving stats: {e}")
        
        return receiving_stats
    
    def _process_defensive_stats(self, stat_category: Dict[str, Any], team_id: int,
                               game_id: str, season: int, week: int) -> List[Dict[str, Any]]:
        """Process defensive statistics"""
        defensive_stats = []
        
        try:
            if 'athletes' not in stat_category:
                return defensive_stats
            
            for athlete in stat_category['athletes']:
                athlete_info = athlete.get('athlete', {})
                name = athlete_info.get('displayName', '')
                position = athlete_info.get('position', {}).get('abbreviation', '')
                
                if not name:
                    continue
                
                player_id = self._get_or_create_player(name, position, team_id)
                if not player_id:
                    continue
                
                # Extract stats
                stats = athlete.get('stats', [])
                stat_values = {stat.split('-')[0].strip(): int(stat.split('-')[1].strip()) 
                              for stat in stats if '-' in stat and stat.split('-')[1].strip().isdigit()}
                
                defensive_stat = {
                    'player_id': player_id,
                    'game_id': game_id,
                    'season': season,
                    'week': week,
                    'tackles_solo': stat_values.get('SOLO', 0),
                    'tackles_assisted': stat_values.get('AST', 0),
                    'tackles_total': stat_values.get('TOT', stat_values.get('SOLO', 0) + stat_values.get('AST', 0)),
                    'sacks': stat_values.get('SACK', 0),
                    'interceptions': stat_values.get('INT', 0),
                    'passes_defended': stat_values.get('PD', 0),
                    'fumbles_forced': stat_values.get('FF', 0),
                    'fumbles_recovered': stat_values.get('FR', 0)
                }
                
                defensive_stats.append(defensive_stat)
        
        except Exception as e:
            self.logger.error(f"Failed to process defensive stats: {e}")
        
        return defensive_stats
    
    def _extract_from_drives(self, drives_data: Dict[str, Any], game_id: str, 
                           season: int, week: int) -> Dict[str, List[Dict[str, Any]]]:
        """Extract additional stats from drive/play data"""
        # This would extract more detailed stats from play-by-play
        # For now, return empty stats
        return {
            'passing': [],
            'rushing': [],
            'receiving': [],
            'defensive': []
        }
    
    def _get_or_create_player(self, name: str, position: str, team_id: int) -> Optional[int]:
        """Get existing player ID or create new player"""
        # This is a simplified version - in production you'd want better player matching
        player_key = (name, position)
        
        if player_key in self.player_mappings:
            return self.player_mappings[player_key]
        
        # For now, return None if player doesn't exist
        # In production, you'd create the player record here
        return None
    
    def extract_season_stats(self, season: int, db_manager) -> int:
        """
        Extract statistics for all games in a season
        
        Args:
            season: Season year
            db_manager: Database manager instance
            
        Returns:
            Number of stat records created
        """
        try:
            # Get all games for the season
            games = db_manager.query(
                "SELECT game_id, nfl_game_id, season, week FROM games WHERE season = ? ORDER BY week",
                (season,)
            )
            
            if not games:
                self.logger.warning(f"No games found for season {season}")
                return 0
            
            self.logger.info(f"Processing {len(games)} games for season {season}")
            
            total_stats = 0
            
            for game in games:
                try:
                    game_stats = self.extract_game_stats(
                        game['nfl_game_id'], 
                        game['season'], 
                        game['week']
                    )
                    
                    # Insert stats into database
                    for category, stats in game_stats.items():
                        if stats:
                            table_name = f"{category}_stats"
                            inserted = db_manager.insert_bulk_data(table_name, stats)
                            total_stats += inserted
                            
                except Exception as e:
                    self.logger.error(f"Failed to process game {game['nfl_game_id']}: {e}")
                    continue
            
            self.logger.info(f"Extracted {total_stats} total stat records for season {season}")
            return total_stats
            
        except Exception as e:
            self.logger.error(f"Failed to extract season stats for {season}: {e}")
            return 0


def extract_stats_for_seasons(seasons: List[int], db_manager) -> Dict[int, int]:
    """
    Extract statistics for multiple seasons
    
    Args:
        seasons: List of season years
        db_manager: Database manager instance
        
    Returns:
        Dictionary mapping season -> number of stats extracted
    """
    extractor = StatsExtractor()
    
    # Get team mappings
    teams_data = db_manager.query("SELECT team_id, team_code FROM teams")
    team_mappings = {row['team_code']: row['team_id'] for row in teams_data}
    
    # Get player mappings (if any exist)
    players_data = db_manager.query("SELECT player_id, name, position FROM players")
    player_mappings = {(row['name'], row['position']): row['player_id'] for row in players_data}
    
    extractor.set_mappings(team_mappings, player_mappings)
    
    results = {}
    for season in seasons:
        results[season] = extractor.extract_season_stats(season, db_manager)
    
    return results