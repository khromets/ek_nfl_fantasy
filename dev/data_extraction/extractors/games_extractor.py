"""
NFL Games data extraction from ESPN API
"""
import requests
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from ..core.config import ESPN_BASE_URL, REQUEST_HEADERS, RATE_LIMITS, SEASONS
from ..core.rate_limiter import get_rate_limiter
from ..core.data_validator import DataValidator

class GamesExtractor:
    """
    Extract NFL game schedules and results from ESPN API
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = get_rate_limiter()
        self.validator = DataValidator()
        self.team_mappings = {}  # Will be populated with team_code -> team_id mapping
    
    def set_team_mappings(self, mappings: Dict[str, int]):
        """
        Set team code to team ID mappings
        
        Args:
            mappings: Dictionary of team_code -> team_id
        """
        self.team_mappings = mappings
        self.logger.info(f"Set team mappings for {len(mappings)} teams")
    
    def extract_games_for_season(self, season: int) -> List[Dict[str, Any]]:
        """
        Extract all games for a specific season
        
        Args:
            season: Season year (e.g., 2023)
            
        Returns:
            List of game dictionaries
        """
        games = []
        
        try:
            self.logger.info(f"Extracting games for {season} season")
            
            # ESPN API endpoint for season schedule
            self.rate_limiter.wait_if_needed('espn_api', RATE_LIMITS['espn_api'])
            
            url = f"{ESPN_BASE_URL}/scoreboard"
            params = {
                'seasontype': '2',  # Regular season
                'year': str(season)
            }
            
            response = requests.get(url, headers=REQUEST_HEADERS, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Process regular season games
            regular_season_games = self._process_scoreboard_data(data, season, 'REG')
            games.extend(regular_season_games)
            
            # Get playoff games if available
            try:
                self.rate_limiter.wait_if_needed('espn_api', RATE_LIMITS['espn_api'])
                
                playoff_params = {
                    'seasontype': '3',  # Playoffs
                    'year': str(season)
                }
                
                playoff_response = requests.get(url, headers=REQUEST_HEADERS, 
                                               params=playoff_params, timeout=30)
                playoff_response.raise_for_status()
                
                playoff_data = playoff_response.json()
                playoff_games = self._process_scoreboard_data(playoff_data, season, 'POST')
                games.extend(playoff_games)
                
                self.logger.info(f"Retrieved {len(playoff_games)} playoff games for {season}")
                
            except Exception as e:
                self.logger.warning(f"Could not retrieve playoff data for {season}: {e}")
            
            # Get additional weeks if needed (ESPN sometimes requires week-by-week requests)
            if len(games) < 250:  # Minimum expected games per season
                additional_games = self._extract_games_by_week(season)
                # Merge games, avoiding duplicates
                existing_game_ids = {game.get('nfl_game_id') for game in games}
                for game in additional_games:
                    if game.get('nfl_game_id') not in existing_game_ids:
                        games.append(game)
            
            self.logger.info(f"Extracted {len(games)} total games for {season} season")
            
        except Exception as e:
            self.logger.error(f"Failed to extract games for season {season}: {e}")
            raise
        
        return games
    
    def _process_scoreboard_data(self, data: Dict[str, Any], season: int, 
                                game_type: str) -> List[Dict[str, Any]]:
        """
        Process ESPN scoreboard API response
        
        Args:
            data: API response data
            season: Season year
            game_type: 'REG', 'POST', etc.
            
        Returns:
            List of processed game dictionaries
        """
        games = []
        
        try:
            if 'events' not in data:
                self.logger.warning("No events found in scoreboard data")
                return games
            
            for event in data['events']:
                game = self._process_single_game(event, season, game_type)
                if game:
                    games.append(game)
            
        except Exception as e:
            self.logger.error(f"Failed to process scoreboard data: {e}")
        
        return games
    
    def _process_single_game(self, event: Dict[str, Any], season: int, 
                            game_type: str) -> Optional[Dict[str, Any]]:
        """
        Process a single game from ESPN API
        
        Args:
            event: Game event data from ESPN
            season: Season year
            game_type: Game type (REG, POST, etc.)
            
        Returns:
            Processed game dictionary or None if invalid
        """
        try:
            # Basic game info
            game_id = event.get('id')
            if not game_id:
                return None
            
            # Date parsing
            date_str = event.get('date')
            if not date_str:
                return None
            
            game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
            
            # Week info
            week = self._extract_week_from_event(event)
            
            # Teams
            competitions = event.get('competitions', [])
            if not competitions:
                return None
            
            competition = competitions[0]
            competitors = competition.get('competitors', [])
            
            if len(competitors) != 2:
                return None
            
            home_team = None
            away_team = None
            home_score = None
            away_score = None
            
            for competitor in competitors:
                team_info = competitor.get('team', {})
                team_abbrev = team_info.get('abbreviation', '').upper()
                
                # Map ESPN abbreviations to our standard
                if team_abbrev == 'WSH':
                    team_abbrev = 'WAS'
                
                score = competitor.get('score')
                if score is not None:
                    score = int(score)
                
                if competitor.get('homeAway') == 'home':
                    home_team = team_abbrev
                    home_score = score
                else:
                    away_team = team_abbrev
                    away_score = score
            
            if not home_team or not away_team:
                self.logger.warning(f"Could not determine teams for game {game_id}")
                return None
            
            # Get team IDs from mappings
            home_team_id = self.team_mappings.get(home_team)
            away_team_id = self.team_mappings.get(away_team)
            
            if not home_team_id or not away_team_id:
                self.logger.warning(f"Could not find team IDs for {home_team} vs {away_team}")
                return None
            
            # Additional game info
            status = competition.get('status', {})
            game_completed = status.get('type', {}).get('completed', False)
            
            # Venue info
            venue = event.get('competitions', [{}])[0].get('venue', {})
            
            return {
                'nfl_game_id': str(game_id),
                'season': season,
                'week': week,
                'game_date': game_date,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'home_score': home_score if game_completed else None,
                'away_score': away_score if game_completed else None,
                'game_type': game_type,
                'completed': game_completed,
                'venue_name': venue.get('fullName'),
                'venue_city': venue.get('address', {}).get('city'),
                'venue_state': venue.get('address', {}).get('state')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to process single game: {e}")
            return None
    
    def _extract_week_from_event(self, event: Dict[str, Any]) -> Optional[int]:
        """
        Extract week number from game event
        
        Args:
            event: Game event data
            
        Returns:
            Week number or None
        """
        try:
            # Try method 1: Direct week property
            if 'week' in event:
                week_data = event['week']
                if isinstance(week_data, dict):
                    week_num = week_data.get('number')
                    if week_num:
                        return int(week_num)
                elif isinstance(week_data, (int, str)):
                    try:
                        return int(week_data)
                    except (ValueError, TypeError):
                        pass
            
            # Try method 2: Competitions -> week
            competitions = event.get('competitions', [])
            if competitions:
                competition = competitions[0]
                
                # Check competition.week
                if 'week' in competition:
                    week_data = competition['week']
                    if isinstance(week_data, dict):
                        week_num = week_data.get('number')
                        if week_num:
                            return int(week_num)
                    elif isinstance(week_data, (int, str)):
                        try:
                            return int(week_data)
                        except (ValueError, TypeError):
                            pass
            
            # Try method 3: Season info
            season_info = event.get('season', {})
            if season_info:
                # Check for week in season
                week = season_info.get('week')
                if week:
                    try:
                        return int(week)
                    except (ValueError, TypeError):
                        pass
                        
                # Check for slug
                slug = season_info.get('slug')
                if slug and slug.isdigit():
                    return int(slug)
            
            # Try method 4: Date-based calculation (fallback)
            date_str = event.get('date')
            if date_str:
                game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
                season_year = event.get('season', {}).get('year')
                
                if season_year:
                    # NFL season typically starts first Thursday of September
                    # This is a rough calculation
                    season_start = datetime(int(season_year), 9, 1).date()
                    
                    # Find first Thursday of September
                    while season_start.weekday() != 3:  # Thursday = 3
                        season_start = season_start.replace(day=season_start.day + 1)
                    
                    # Calculate weeks since season start
                    days_diff = (game_date - season_start).days
                    week_num = (days_diff // 7) + 1
                    
                    # Sanity check: NFL weeks are 1-22
                    if 1 <= week_num <= 22:
                        return int(week_num)
            
            # Try method 5: Extract from text strings
            name = event.get('name', '')
            short_name = event.get('shortName', '')
            
            for text in [name, short_name]:
                if 'Week' in text:
                    words = text.split()
                    for i, word in enumerate(words):
                        if word == 'Week' and i + 1 < len(words):
                            week_str = words[i + 1]
                            if week_str.isdigit():
                                return int(week_str)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Failed to extract week from event: {e}")
            return None
    
    def _extract_games_by_week(self, season: int, max_week: int = 22) -> List[Dict[str, Any]]:
        """
        Extract games by requesting each week individually
        
        Args:
            season: Season year
            max_week: Maximum week to try
            
        Returns:
            List of game dictionaries
        """
        games = []
        
        for week in range(1, max_week + 1):
            try:
                self.rate_limiter.wait_if_needed('espn_api', RATE_LIMITS['espn_api'])
                
                url = f"{ESPN_BASE_URL}/scoreboard"
                params = {
                    'seasontype': '2',  # Regular season
                    'year': str(season),
                    'week': str(week)
                }
                
                response = requests.get(url, headers=REQUEST_HEADERS, 
                                       params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                week_games = self._process_scoreboard_data(data, season, 'REG')
                
                if not week_games:
                    # If no games found for this week, might be end of season
                    if week > 18:  # Regular season typically ends at week 18
                        break
                
                games.extend(week_games)
                self.logger.debug(f"Extracted {len(week_games)} games for week {week}")
                
            except Exception as e:
                self.logger.warning(f"Failed to get games for {season} week {week}: {e}")
                if week <= 18:  # Continue trying for regular season weeks
                    continue
                else:
                    break
        
        return games
    
    def extract_games_for_seasons(self, seasons: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """
        Extract games for multiple seasons
        
        Args:
            seasons: List of season years
            
        Returns:
            Dictionary mapping season -> list of games
        """
        all_games = {}
        
        for season in seasons:
            try:
                games = self.extract_games_for_season(season)
                
                # Validate games for this season
                self.validator.reset_validation_state()
                is_valid = self.validator.validate_game_data(games, season)
                
                if not is_valid:
                    self.logger.error(f"Game data validation failed for season {season}")
                    self.validator.log_validation_results()
                
                all_games[season] = games
                
            except Exception as e:
                self.logger.error(f"Failed to extract games for season {season}: {e}")
                all_games[season] = []
        
        return all_games


def extract_games(seasons: List[int] = None, 
                 team_mappings: Dict[str, int] = None) -> Dict[int, List[Dict[str, Any]]]:
    """
    Convenience function to extract NFL games
    
    Args:
        seasons: List of seasons to extract (defaults to SEASONS from config)
        team_mappings: Team code to team ID mappings
        
    Returns:
        Dictionary mapping season -> list of games
    """
    if seasons is None:
        seasons = SEASONS
    
    extractor = GamesExtractor()
    
    if team_mappings:
        extractor.set_team_mappings(team_mappings)
    
    return extractor.extract_games_for_seasons(seasons)