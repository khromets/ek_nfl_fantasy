"""
NFL Players roster extraction from Pro Football Reference
"""
import requests
import logging
import time
from typing import List, Dict, Any, Optional, Set
from bs4 import BeautifulSoup
import re
from ..core.config import (
    PRO_FOOTBALL_REFERENCE_BASE, REQUEST_HEADERS, RATE_LIMITS, 
    SEASONS, NFL_TEAMS, PFR_URLS
)
from ..core.rate_limiter import get_adaptive_rate_limiter
from ..core.data_validator import DataValidator

class PlayersExtractor:
    """
    Extract NFL player rosters from Pro Football Reference
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = get_adaptive_rate_limiter()
        self.validator = DataValidator()
        self.team_mappings = {}  # team_code -> team_id
        self.extracted_players = set()  # Track unique players to avoid duplicates
    
    def set_team_mappings(self, mappings: Dict[str, int]):
        """Set team code to team ID mappings"""
        self.team_mappings = mappings
        self.logger.info(f"Set team mappings for {len(mappings)} teams")
    
    def extract_team_roster(self, team_code: str, season: int) -> List[Dict[str, Any]]:
        """
        Extract roster for a specific team and season from Pro Football Reference
        
        Args:
            team_code: Team abbreviation (e.g., 'KC', 'SF')
            season: Season year
            
        Returns:
            List of player dictionaries
        """
        players = []
        
        try:
            # Convert team code to PFR format
            pfr_team_code = self._convert_to_pfr_team_code(team_code)
            
            url = PFR_URLS['team_roster'].format(team=pfr_team_code.lower(), year=season)
            self.logger.info(f"Extracting roster for {team_code} {season}: {url}")
            
            # Rate limit request
            self.rate_limiter.wait_for_request('pro_football_ref')
            
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
            success = response.status_code == 200
            self.rate_limiter.wait_for_request('pro_football_ref', success, response.status_code)
            
            if not success:
                self.logger.error(f"Failed to fetch roster for {team_code} {season}: HTTP {response.status_code}")
                return players
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find roster table
            roster_table = soup.find('table', {'id': 'roster'})
            if not roster_table:
                self.logger.warning(f"No roster table found for {team_code} {season}")
                return players
            
            # Process each player row
            tbody = roster_table.find('tbody')
            if not tbody:
                return players
            
            for row in tbody.find_all('tr'):
                player = self._process_player_row(row, team_code, season)
                if player:
                    # Check for duplicates using name + position + season combo
                    player_key = (player['name'], player['position'], season)
                    if player_key not in self.extracted_players:
                        players.append(player)
                        self.extracted_players.add(player_key)
                    else:
                        self.logger.debug(f"Duplicate player found: {player['name']} {player['position']}")
            
            self.logger.info(f"Extracted {len(players)} players from {team_code} {season} roster")
            
        except Exception as e:
            self.logger.error(f"Failed to extract roster for {team_code} {season}: {e}")
        
        return players
    
    def _convert_to_pfr_team_code(self, team_code: str) -> str:
        """
        Convert standard team code to Pro Football Reference team code
        
        Args:
            team_code: Standard team code (e.g., 'WAS')
            
        Returns:
            PFR team code
        """
        # Pro Football Reference uses different team codes for some teams
        pfr_mappings = {
            'WAS': 'was',  # Washington
            'LV': 'rai',   # Las Vegas Raiders (still listed as Raiders on PFR)
            'LAR': 'ram',  # Los Angeles Rams
            'LAC': 'sdg',  # Los Angeles Chargers (sometimes still San Diego on older pages)
            'KC': 'kan',   # Kansas City Chiefs
            'GB': 'gnb',   # Green Bay Packers
            'NE': 'nwe',   # New England Patriots
            'NO': 'nor',   # New Orleans Saints
            'NYG': 'nyg',  # New York Giants
            'NYJ': 'nyj',  # New York Jets
            'SF': 'sfo',   # San Francisco 49ers
            'TB': 'tam',   # Tampa Bay Buccaneers
        }
        
        return pfr_mappings.get(team_code, team_code.lower())
    
    def _process_player_row(self, row, team_code: str, season: int) -> Optional[Dict[str, Any]]:
        """
        Process a single player row from roster table
        
        Args:
            row: BeautifulSoup table row
            team_code: Team code
            season: Season year
            
        Returns:
            Player dictionary or None if invalid
        """
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 6:  # Minimum expected columns
                return None
            
            # Extract basic info - column indices may vary by year
            jersey_num = self._safe_extract_text(cells[0])
            name = self._safe_extract_text(cells[1])
            position = self._safe_extract_text(cells[2])
            
            if not name or not position:
                return None
            
            # Clean up name - remove any links or extra formatting
            name_link = cells[1].find('a')
            if name_link:
                name = name_link.get_text().strip()
                player_url = name_link.get('href', '')
            else:
                player_url = ''
            
            # Extract physical stats (age, height, weight)
            age = self._safe_extract_text(cells[3]) if len(cells) > 3 else ''
            height = self._safe_extract_text(cells[4]) if len(cells) > 4 else ''
            weight = self._safe_extract_text(cells[5]) if len(cells) > 5 else ''
            
            # Try to extract college and experience if available
            college = ''
            experience = ''
            
            if len(cells) > 6:
                college = self._safe_extract_text(cells[6])
            if len(cells) > 7:
                experience = self._safe_extract_text(cells[7])
            
            # Convert height to inches
            height_inches = self._convert_height_to_inches(height)
            
            # Extract weight as integer
            weight_lbs = self._extract_weight_pounds(weight)
            
            # Generate player ID from PFR URL if available
            pfr_player_id = self._extract_player_id_from_url(player_url)
            
            # Get team ID
            team_id = self.team_mappings.get(team_code)
            if not team_id:
                self.logger.warning(f"No team ID found for {team_code}")
            
            return {
                'name': name.strip(),
                'position': self._normalize_position(position),
                'team_id': team_id,
                'jersey_number': self._safe_int_convert(jersey_num),
                'height_inches': height_inches,
                'weight_lbs': weight_lbs,
                'college': college.strip() if college else None,
                'pfr_player_id': pfr_player_id,
                'pfr_url': player_url,
                'season_extracted': season,
                'age_at_extraction': self._safe_int_convert(age)
            }
            
        except Exception as e:
            self.logger.debug(f"Failed to process player row: {e}")
            return None
    
    def _safe_extract_text(self, cell) -> str:
        """Safely extract text from a table cell"""
        if cell is None:
            return ''
        return cell.get_text().strip()
    
    def _safe_int_convert(self, value: str) -> Optional[int]:
        """Safely convert string to integer"""
        if not value or value == '':
            return None
        try:
            return int(value)
        except ValueError:
            return None
    
    def _normalize_position(self, position: str) -> str:
        """
        Normalize position abbreviations
        
        Args:
            position: Raw position string
            
        Returns:
            Normalized position code
        """
        position = position.upper().strip()
        
        # Handle multiple positions
        if '/' in position:
            # Take first position if multiple listed
            position = position.split('/')[0]
        
        # Common position mappings
        position_mappings = {
            'HB': 'RB',     # Halfback -> Running Back
            'FB': 'RB',     # Fullback -> Running Back  
            'ILB': 'LB',    # Inside Linebacker -> Linebacker
            'OLB': 'LB',    # Outside Linebacker -> Linebacker
            'MLB': 'LB',    # Middle Linebacker -> Linebacker
            'FS': 'S',      # Free Safety -> Safety
            'SS': 'S',      # Strong Safety -> Safety
            'NT': 'DT',     # Nose Tackle -> Defensive Tackle
            'OT': 'T',      # Offensive Tackle
            'OG': 'G',      # Offensive Guard
            'C': 'C',       # Center
            'T': 'T',       # Tackle
            'G': 'G',       # Guard
        }
        
        return position_mappings.get(position, position)
    
    def _convert_height_to_inches(self, height_str: str) -> Optional[int]:
        """
        Convert height string to inches
        
        Args:
            height_str: Height in format like "6-2" or "6'2"
            
        Returns:
            Height in inches or None
        """
        if not height_str:
            return None
        
        # Match patterns like "6-2", "6'2", "6 2", etc.
        match = re.match(r'(\d+)[-\'\"\\s](\d+)', height_str.strip())
        if match:
            feet = int(match.group(1))
            inches = int(match.group(2))
            return feet * 12 + inches
        
        return None
    
    def _extract_weight_pounds(self, weight_str: str) -> Optional[int]:
        """
        Extract weight in pounds from weight string
        
        Args:
            weight_str: Weight string like "225" or "225 lbs"
            
        Returns:
            Weight in pounds or None
        """
        if not weight_str:
            return None
        
        # Extract numbers from weight string
        numbers = re.findall(r'\d+', weight_str)
        if numbers:
            return int(numbers[0])
        
        return None
    
    def _extract_player_id_from_url(self, url: str) -> Optional[str]:
        """
        Extract player ID from Pro Football Reference URL
        
        Args:
            url: Player URL path
            
        Returns:
            Player ID or None
        """
        if not url:
            return None
        
        # URL format is typically /players/A/AbcdEf01.htm
        match = re.search(r'/players/[A-Z]/([A-Za-z0-9]+)\.htm', url)
        if match:
            return match.group(1)
        
        return None
    
    def extract_all_rosters_for_season(self, season: int) -> List[Dict[str, Any]]:
        """
        Extract rosters for all teams for a specific season
        
        Args:
            season: Season year
            
        Returns:
            List of all players across all teams
        """
        all_players = []
        
        self.logger.info(f"Extracting rosters for all teams in {season} season")
        
        for team_code in NFL_TEAMS.keys():
            try:
                team_players = self.extract_team_roster(team_code, season)
                all_players.extend(team_players)
                
                # Add delay between teams to be respectful
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to extract roster for {team_code} {season}: {e}")
        
        # Validate the extracted data
        self.validator.reset_validation_state()
        is_valid = self.validator.validate_player_data(all_players)
        
        if not is_valid:
            self.logger.error(f"Player data validation failed for season {season}")
            self.validator.log_validation_results()
        else:
            self.validator.log_validation_results()
        
        self.logger.info(f"Extracted {len(all_players)} total players for {season} season")
        return all_players
    
    def extract_rosters_for_seasons(self, seasons: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """
        Extract rosters for multiple seasons
        
        Args:
            seasons: List of season years
            
        Returns:
            Dictionary mapping season -> list of players
        """
        all_rosters = {}
        
        for season in seasons:
            try:
                players = self.extract_all_rosters_for_season(season)
                all_rosters[season] = players
                
                self.logger.info(f"Completed roster extraction for {season}: {len(players)} players")
                
            except Exception as e:
                self.logger.error(f"Failed to extract rosters for season {season}: {e}")
                all_rosters[season] = []
        
        return all_rosters


def extract_players(seasons: List[int] = None, 
                   team_mappings: Dict[str, int] = None) -> Dict[int, List[Dict[str, Any]]]:
    """
    Convenience function to extract NFL player rosters
    
    Args:
        seasons: List of seasons to extract (defaults to SEASONS from config)
        team_mappings: Team code to team ID mappings
        
    Returns:
        Dictionary mapping season -> list of players
    """
    if seasons is None:
        seasons = SEASONS
    
    extractor = PlayersExtractor()
    
    if team_mappings:
        extractor.set_team_mappings(team_mappings)
    
    return extractor.extract_rosters_for_seasons(seasons)