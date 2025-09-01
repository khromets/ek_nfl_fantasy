"""
NFL Teams data extraction
"""
import requests
import logging
from typing import List, Dict, Any, Optional
from ..core.config import ESPN_BASE_URL, NFL_TEAMS, REQUEST_HEADERS, RATE_LIMITS
from ..core.rate_limiter import get_rate_limiter
from ..core.data_validator import DataValidator

class TeamsExtractor:
    """
    Extract NFL team information from ESPN API
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = get_rate_limiter()
        self.validator = DataValidator()
    
    def extract_teams_from_espn(self) -> List[Dict[str, Any]]:
        """
        Extract team data from ESPN API
        
        Returns:
            List of team dictionaries
        """
        teams = []
        
        try:
            # Rate limit the request
            self.rate_limiter.wait_if_needed('espn_api', RATE_LIMITS['espn_api'])
            
            # Make request to ESPN API
            url = f"{ESPN_BASE_URL}/teams"
            response = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Retrieved team data from ESPN API")
            
            # Process teams from ESPN response
            if 'sports' in data and len(data['sports']) > 0:
                sport = data['sports'][0]
                if 'leagues' in sport and len(sport['leagues']) > 0:
                    league = sport['leagues'][0]
                    if 'teams' in league:
                        for team_data in league['teams']:
                            team = self._process_espn_team(team_data['team'])
                            if team:
                                teams.append(team)
            
            self.logger.info(f"Processed {len(teams)} teams from ESPN API")
            
        except Exception as e:
            self.logger.error(f"Failed to extract teams from ESPN API: {e}")
            # Fall back to static team data
            teams = self._get_static_teams()
        
        return teams
    
    def _process_espn_team(self, team_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process individual team from ESPN API response
        
        Args:
            team_data: Team data from ESPN API
            
        Returns:
            Processed team dictionary or None if invalid
        """
        try:
            # Extract team abbreviation
            abbreviation = team_data.get('abbreviation', '').upper()
            
            # Map some ESPN abbreviations to our standard codes
            espn_to_standard = {
                'WSH': 'WAS',  # Washington
                'LV': 'LV',    # Las Vegas (already correct)
            }
            
            team_code = espn_to_standard.get(abbreviation, abbreviation)
            
            # Skip if not a valid NFL team
            if team_code not in NFL_TEAMS:
                self.logger.warning(f"Unknown team code from ESPN: {abbreviation}")
                return None
            
            # Get team details from our config
            team_info = NFL_TEAMS[team_code]
            
            return {
                'team_code': team_code,
                'team_name': team_data.get('displayName', team_info['name']),
                'conference': team_info['conference'],
                'division': team_info['division'],
                'espn_id': team_data.get('id'),
                'location': team_data.get('location', ''),
                'nickname': team_data.get('name', ''),
                'color': team_data.get('color', ''),
                'alternate_color': team_data.get('alternateColor', ''),
                'logo_url': self._extract_logo_url(team_data.get('logos', []))
            }
            
        except Exception as e:
            self.logger.error(f"Failed to process ESPN team data: {e}")
            return None
    
    def _extract_logo_url(self, logos: List[Dict[str, Any]]) -> str:
        """
        Extract primary logo URL from logos list
        
        Args:
            logos: List of logo dictionaries from ESPN
            
        Returns:
            Logo URL string
        """
        if not logos:
            return ''
        
        # Try to find the primary logo
        for logo in logos:
            if logo.get('rel') and 'default' in logo['rel']:
                return logo.get('href', '')
        
        # Fallback to first logo
        return logos[0].get('href', '') if logos else ''
    
    def _get_static_teams(self) -> List[Dict[str, Any]]:
        """
        Get static team data as fallback
        
        Returns:
            List of team dictionaries from static data
        """
        self.logger.info("Using static team data as fallback")
        
        teams = []
        for team_code, info in NFL_TEAMS.items():
            teams.append({
                'team_code': team_code,
                'team_name': info['name'],
                'conference': info['conference'],
                'division': info['division'],
                'espn_id': None,
                'location': '',
                'nickname': '',
                'color': '',
                'alternate_color': '',
                'logo_url': ''
            })
        
        return teams
    
    def extract_and_validate_teams(self) -> List[Dict[str, Any]]:
        """
        Extract teams and validate the data
        
        Returns:
            List of validated team dictionaries
        """
        # Extract teams
        teams = self.extract_teams_from_espn()
        
        # Validate teams
        self.validator.reset_validation_state()
        is_valid = self.validator.validate_team_data(teams)
        
        if not is_valid:
            self.logger.error("Team data validation failed")
            self.validator.log_validation_results()
            
            # Try static fallback if ESPN data is invalid
            if len(teams) != 32:
                self.logger.info("Falling back to static team data due to validation failure")
                teams = self._get_static_teams()
                
                # Validate static data
                self.validator.reset_validation_state()
                is_valid = self.validator.validate_team_data(teams)
                
                if not is_valid:
                    self.logger.error("Even static team data failed validation")
                    self.validator.log_validation_results()
                    raise Exception("Unable to get valid team data")
        
        self.validator.log_validation_results()
        self.logger.info(f"Successfully extracted and validated {len(teams)} teams")
        
        return teams
    
    def get_team_mappings(self) -> Dict[str, int]:
        """
        Get mapping of team codes to team IDs (after teams are in database)
        
        Returns:
            Dictionary mapping team_code to team_id
        """
        # This would be called after teams are inserted into database
        # For now, return empty dict - will be implemented when database operations are added
        return {}


def extract_teams() -> List[Dict[str, Any]]:
    """
    Convenience function to extract NFL teams
    
    Returns:
        List of team dictionaries
    """
    extractor = TeamsExtractor()
    return extractor.extract_and_validate_teams()


if __name__ == "__main__":
    # Test the extractor
    import logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        teams = extract_teams()
        print(f"Successfully extracted {len(teams)} teams:")
        for team in teams[:3]:  # Show first 3 teams
            print(f"  {team['team_code']}: {team['team_name']} ({team['conference']} {team['division']})")
        print("...")
        
    except Exception as e:
        print(f"Failed to extract teams: {e}")