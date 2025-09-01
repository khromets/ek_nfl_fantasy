#!/usr/bin/env python3
"""
Pro Football Reference Scraper - Modular NFL Statistics Extraction
Phase 1.2 - Step 2: Technical Infrastructure
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime

# Import existing infrastructure
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_extraction', 'core'))

try:
    from rate_limiter import get_adaptive_rate_limiter, RateLimiter
    from data_validator import DataValidator
    INFRASTRUCTURE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import infrastructure modules: {e}")
    INFRASTRUCTURE_AVAILABLE = False
    # Fallback rate limiter
    class RateLimiter:
        def __init__(self):
            self.last_request = {}
        
        def wait_if_needed(self, domain, interval):
            current_time = time.time()
            if domain in self.last_request:
                elapsed = current_time - self.last_request[domain]
                if elapsed < interval:
                    sleep_time = interval - elapsed
                    time.sleep(sleep_time)
            self.last_request[domain] = time.time()
    
    def get_adaptive_rate_limiter():
        return RateLimiter()
    
    class DataValidator:
        def __init__(self):
            pass

class ProFootballReferenceScraper:
    """
    Modular scraper for Pro Football Reference statistics
    Supports season stats and individual game logs
    """
    
    def __init__(self):
        self.base_url = "https://www.pro-football-reference.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        if INFRASTRUCTURE_AVAILABLE:
            self.rate_limiter = get_adaptive_rate_limiter()
            self.validator = DataValidator()
        else:
            self.rate_limiter = RateLimiter()
            self.validator = DataValidator()
        self.logger = logging.getLogger(__name__)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        self.domain = "pro-football-reference.com"
    
    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """
        Make rate-limited request to PFR
        
        Args:
            url: URL to request
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            # Use rate limiting (adaptive if available, simple if not)
            if INFRASTRUCTURE_AVAILABLE and hasattr(self.rate_limiter, 'wait_for_request'):
                self.rate_limiter.wait_for_request(self.domain, success=True)
            else:
                self.rate_limiter.wait_if_needed(self.domain, 2.0)
            
            self.logger.info(f"Requesting: {url}")
            response = self.session.get(url, timeout=15)
            
            # Update rate limiter based on response
            if INFRASTRUCTURE_AVAILABLE and hasattr(self.rate_limiter, 'wait_for_request'):
                success = response.status_code == 200
                self.rate_limiter.wait_for_request(
                    self.domain, 
                    success=success, 
                    status_code=response.status_code
                )
            
            if response.status_code == 200:
                return BeautifulSoup(response.content, 'html.parser')
            else:
                self.logger.error(f"HTTP {response.status_code} for {url}")
                return None
                
        except Exception as e:
            self.logger.error(f"Request failed for {url}: {e}")
            if INFRASTRUCTURE_AVAILABLE and hasattr(self.rate_limiter, 'wait_for_request'):
                self.rate_limiter.wait_for_request(self.domain, success=False)
            return None
    
    def _extract_stats_table(self, soup: BeautifulSoup, table_id: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Extract statistics table from PFR page
        
        Args:
            soup: BeautifulSoup object
            table_id: Specific table ID to extract (optional)
            
        Returns:
            DataFrame with extracted data or None
        """
        try:
            # Find stats table
            if table_id:
                table = soup.find('table', {'id': table_id})
            else:
                table = soup.find('table', {'class': 'stats_table'})
                if not table:
                    # Try to find any table with data
                    tables = soup.find_all('table')
                    table = tables[0] if tables else None
            
            if not table:
                self.logger.warning("No stats table found")
                return None
            
            # Extract headers
            header_row = table.find('thead')
            if not header_row:
                header_row = table.find('tr')
            
            if not header_row:
                self.logger.warning("No header row found in table")
                return None
            
            # Get column names
            headers = []
            header_cells = header_row.find_all(['th', 'td'])
            for cell in header_cells:
                text = cell.get_text(strip=True)
                # Skip empty headers or rank columns
                if text and text not in ['', 'Rk']:
                    headers.append(text)
                elif not text:
                    headers.append(f'col_{len(headers)}')  # Placeholder for empty headers
            
            if not headers:
                self.logger.warning("No valid headers found")
                return None
            
            # Extract data rows
            rows = []
            tbody = table.find('tbody')
            if tbody:
                data_rows = tbody.find_all('tr')
            else:
                # Skip header row and get rest
                data_rows = table.find_all('tr')[1:]
            
            for row in data_rows:
                # Skip header rows that might be in tbody
                if row.get('class') and 'thead' in row.get('class'):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue
                
                row_data = []
                player_link = None
                
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    
                    # Extract player link if present (check all cells)
                    link = cell.find('a')
                    if link and link.get('href') and '/players/' in link.get('href'):
                        player_link = link.get('href')
                    
                    # Convert numeric fields
                    if text.replace('-', '').replace('.', '').isdigit():
                        try:
                            if '.' in text:
                                text = float(text)
                            else:
                                text = int(text) if text != '-' else 0
                        except ValueError:
                            pass  # Keep as string
                    elif text == '':
                        text = None
                    
                    row_data.append(text)
                
                # Add player_link as extra column if found
                if player_link:
                    row_data.append(player_link)
                
                if row_data:
                    rows.append(row_data)
            
            if not rows:
                self.logger.warning("No data rows found")
                return None
            
            # Create DataFrame
            # Add player_link column if we found any player links
            if any(len(row) > len(headers) for row in rows):
                headers.append('player_link')
                # Pad shorter rows
                for row in rows:
                    while len(row) < len(headers):
                        row.append(None)
            
            # Ensure all rows have same length as headers
            for i, row in enumerate(rows):
                if len(row) != len(headers):
                    # Pad or trim to match headers
                    if len(row) < len(headers):
                        rows[i] = row + [None] * (len(headers) - len(row))
                    else:
                        rows[i] = row[:len(headers)]
            
            df = pd.DataFrame(rows, columns=headers)
            
            self.logger.info(f"Extracted table with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract stats table: {e}")
            return None
    
    def get_season_stats(self, year: int, stat_type: str) -> Optional[pd.DataFrame]:
        """
        Extract season statistics for a given year and stat type
        
        Args:
            year: Season year (e.g., 2024)
            stat_type: Type of stats ('passing', 'rushing', 'receiving', 'defense')
            
        Returns:
            DataFrame with season statistics
        """
        # Map stat types to PFR URLs
        stat_urls = {
            'passing': f'/years/{year}/passing.htm',
            'rushing': f'/years/{year}/rushing.htm',
            'receiving': f'/years/{year}/receiving.htm',
            'defense': f'/years/{year}/defense.htm',
            'kicking': f'/years/{year}/kicking.htm'
        }
        
        if stat_type not in stat_urls:
            self.logger.error(f"Unsupported stat type: {stat_type}")
            return None
        
        url = self.base_url + stat_urls[stat_type]
        soup = self._make_request(url)
        
        if not soup:
            return None
        
        # Extract the main stats table
        df = self._extract_stats_table(soup)
        if df is not None:
            # Add metadata columns
            df['season'] = year
            df['stat_type'] = stat_type
            df['extracted_at'] = datetime.now()
        
        return df
    
    def get_player_game_log(self, player_url: str, year: int) -> Optional[pd.DataFrame]:
        """
        Extract individual player game log
        
        Args:
            player_url: Player's PFR URL path (e.g., '/players/P/PeniMi00.htm')
            year: Season year
            
        Returns:
            DataFrame with game-by-game statistics
        """
        # Construct game log URL
        # Remove .htm and add /gamelog/year/
        player_id = player_url.replace('/players/', '').replace('.htm', '')
        gamelog_url = f"{self.base_url}/players/{player_id}/gamelog/{year}/"
        
        soup = self._make_request(gamelog_url)
        if not soup:
            return None
        
        # Extract game log table
        df = self._extract_stats_table(soup, table_id='stats')
        if df is not None:
            # Add metadata
            df['player_url'] = player_url
            df['season'] = year
            df['extracted_at'] = datetime.now()
        
        return df
    
    def extract_player_urls_from_stats(self, df: pd.DataFrame) -> List[str]:
        """
        Extract player URLs from a stats DataFrame
        
        Args:
            df: DataFrame with player_link column
            
        Returns:
            List of player URL paths
        """
        if 'player_link' not in df.columns:
            return []
        
        urls = []
        for link in df['player_link'].dropna():
            if isinstance(link, str) and link.startswith('/players/'):
                urls.append(link)
        
        return list(set(urls))  # Remove duplicates

class PositionSpecificExtractor:
    """
    Position-specific extraction logic for fantasy-relevant statistics
    """
    
    def __init__(self, scraper: ProFootballReferenceScraper):
        self.scraper = scraper
        self.logger = logging.getLogger(__name__)
    
    def extract_qb_stats(self, year: int) -> Optional[pd.DataFrame]:
        """Extract quarterback statistics"""
        self.logger.info(f"Extracting QB stats for {year}")
        return self.scraper.get_season_stats(year, 'passing')
    
    def extract_rb_stats(self, year: int) -> Optional[pd.DataFrame]:
        """Extract running back statistics"""
        self.logger.info(f"Extracting RB stats for {year}")
        return self.scraper.get_season_stats(year, 'rushing')
    
    def extract_wr_te_stats(self, year: int) -> Optional[pd.DataFrame]:
        """Extract wide receiver and tight end statistics"""
        self.logger.info(f"Extracting WR/TE stats for {year}")
        return self.scraper.get_season_stats(year, 'receiving')
    
    def extract_def_stats(self, year: int) -> Optional[pd.DataFrame]:
        """Extract defensive statistics"""
        self.logger.info(f"Extracting defensive stats for {year}")
        return self.scraper.get_season_stats(year, 'defense')
    
    def extract_position_game_logs(self, year: int, position: str, max_players: int = 50) -> Dict[str, pd.DataFrame]:
        """
        Extract game logs for top players at a position
        
        Args:
            year: Season year
            position: Position ('QB', 'RB', 'WR', 'TE')
            max_players: Maximum number of players to extract
            
        Returns:
            Dictionary mapping player URLs to their game log DataFrames
        """
        position_map = {
            'QB': 'passing',
            'RB': 'rushing', 
            'WR': 'receiving',
            'TE': 'receiving'
        }
        
        stat_type = position_map.get(position)
        if not stat_type:
            self.logger.error(f"Unsupported position: {position}")
            return {}
        
        # Get season stats to find top players
        season_stats = self.scraper.get_season_stats(year, stat_type)
        if season_stats is None:
            return {}
        
        # Filter by position if receiving stats (to separate WR/TE)
        if stat_type == 'receiving' and 'Pos' in season_stats.columns:
            if position == 'WR':
                season_stats = season_stats[season_stats['Pos'] == 'WR']
            elif position == 'TE':
                season_stats = season_stats[season_stats['Pos'] == 'TE']
        
        # Get player URLs
        player_urls = self.scraper.extract_player_urls_from_stats(season_stats)
        player_urls = player_urls[:max_players]  # Limit to top N players
        
        self.logger.info(f"Extracting game logs for {len(player_urls)} {position} players")
        
        game_logs = {}
        for i, player_url in enumerate(player_urls):
            self.logger.info(f"Processing {position} player {i+1}/{len(player_urls)}: {player_url}")
            
            game_log = self.scraper.get_player_game_log(player_url, year)
            if game_log is not None:
                game_logs[player_url] = game_log
            
            # Progress update
            if (i + 1) % 10 == 0:
                self.logger.info(f"Completed {i+1}/{len(player_urls)} {position} players")
        
        return game_logs

def main():
    """Test the scraper functionality"""
    print("Testing Pro Football Reference Scraper")
    print("=" * 50)
    
    # Initialize scraper
    scraper = ProFootballReferenceScraper()
    position_extractor = PositionSpecificExtractor(scraper)
    
    # Test season stats extraction
    print("\n1. Testing season stats extraction...")
    qb_stats = position_extractor.extract_qb_stats(2024)
    if qb_stats is not None:
        print(f"✅ QB Stats: {len(qb_stats)} players")
        print(f"Columns: {list(qb_stats.columns)}")
        if not qb_stats.empty:
            print(f"Sample: {qb_stats.iloc[0]['Player']} - {qb_stats.iloc[0].get('Yds', 'N/A')} yards")
    else:
        print("❌ QB Stats extraction failed")
    
    # Test player URLs extraction
    if qb_stats is not None:
        print("\n2. Testing player URL extraction...")
        player_urls = scraper.extract_player_urls_from_stats(qb_stats)
        print(f"✅ Found {len(player_urls)} player URLs")
        if player_urls:
            print(f"Sample URL: {player_urls[0]}")
    
    # Test individual game log (with first player)
    if qb_stats is not None and player_urls:
        print("\n3. Testing game log extraction...")
        sample_url = player_urls[0]
        game_log = scraper.get_player_game_log(sample_url, 2024)
        if game_log is not None:
            print(f"✅ Game Log: {len(game_log)} games")
            print(f"Columns: {list(game_log.columns)}")
        else:
            print("❌ Game log extraction failed")
    
    print("\n" + "=" * 50)
    print("Scraper testing completed!")

if __name__ == "__main__":
    main()