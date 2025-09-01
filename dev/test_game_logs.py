#!/usr/bin/env python3
"""
Test individual player game log extraction with manual URLs
Phase 1.2 - Step 3: Test game log extraction
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import json

class ManualGameLogExtractor:
    """Test game log extraction with known player URLs"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.base_url = "https://www.pro-football-reference.com"
    
    def extract_game_log(self, player_url: str, year: int = 2024) -> pd.DataFrame:
        """Extract game log for a specific player"""
        
        # Convert player page URL to game log URL
        if player_url.endswith('.htm'):
            player_id = player_url.replace('/players/', '').replace('.htm', '')
        else:
            player_id = player_url.replace('/players/', '')
        
        gamelog_url = f"{self.base_url}/players/{player_id}/gamelog/{year}/"
        
        print(f"🔍 Extracting game log: {gamelog_url}")
        
        try:
            time.sleep(2)  # Be respectful
            response = self.session.get(gamelog_url, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Failed to get game log: HTTP {response.status_code}")
                return pd.DataFrame()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find game log table (usually the first table)
            tables = soup.find_all('table')
            if not tables:
                print(f"❌ No tables found on game log page")
                return pd.DataFrame()
            
            # Use first table which should be the game log
            table = tables[0]
            
            # Extract headers
            header_rows = table.find_all('tr')
            if not header_rows:
                print(f"❌ No rows found in game log table")
                return pd.DataFrame()
            
            # PFR game logs often have multi-level headers
            # Look for the actual column headers
            headers = []
            header_found = False
            
            for row in header_rows:
                cells = row.find_all(['th', 'td'])
                if not header_found:
                    row_text = [cell.get_text(strip=True) for cell in cells]
                    # Look for typical game log headers
                    if any(header in row_text for header in ['Week', 'Date', 'Opp', 'Result']):
                        headers = row_text
                        header_found = True
                        break
            
            if not headers:
                print(f"❌ Could not find valid headers in game log")
                return pd.DataFrame()
            
            print(f"📊 Found {len(headers)} columns: {headers[:10]}...")
            
            # Extract data rows
            data_rows = []
            data_started = False
            
            for row in header_rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue
                
                # Skip until we find data rows (after headers)
                row_text = [cell.get_text(strip=True) for cell in cells]
                
                # Skip header rows
                if any(header in row_text for header in ['Week', 'Date', 'Passing', 'Rushing']):
                    continue
                
                # Look for actual game data (week numbers, dates, etc.)
                if len(row_text) >= len(headers) and row_text[0].isdigit():
                    # Pad or trim row to match headers
                    if len(row_text) < len(headers):
                        row_text.extend([None] * (len(headers) - len(row_text)))
                    else:
                        row_text = row_text[:len(headers)]
                    
                    data_rows.append(row_text)
            
            if not data_rows:
                print(f"❌ No data rows found in game log")
                return pd.DataFrame()
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers)
            
            # Add metadata
            df['player_url'] = player_url
            df['season'] = year
            df['extracted_at'] = datetime.now()
            
            print(f"✅ Extracted {len(df)} games from game log")
            return df
            
        except Exception as e:
            print(f"❌ Error extracting game log: {e}")
            return pd.DataFrame()

def test_known_players():
    """Test game log extraction with known top players"""
    
    print("🏈 Testing Game Log Extraction")
    print("=" * 40)
    
    extractor = ManualGameLogExtractor()
    
    # Test players from different positions
    test_players = {
        'Joe Burrow (QB)': '/players/B/BurrJo01.htm',
        'Saquon Barkley (RB)': '/players/B/BarkSa00.htm',
        'Justin Jefferson (WR)': '/players/J/JeffJu00.htm',
        'Travis Kelce (TE)': '/players/K/KelcTr00.htm'
    }
    
    results = {}
    
    for player_name, player_url in test_players.items():
        print(f"\n🎯 Testing: {player_name}")
        
        game_log = extractor.extract_game_log(player_url, 2024)
        
        if not game_log.empty:
            print(f"✅ Success: {len(game_log)} games extracted")
            
            # Show sample game data
            if len(game_log) > 0:
                print(f"📊 Columns: {list(game_log.columns)[:10]}...")
                
                # Look for key stats
                sample_game = game_log.iloc[0]
                game_info = []
                
                for col in ['Week', 'Date', 'Opp', 'Result']:
                    if col in game_log.columns:
                        value = sample_game.get(col, 'N/A')
                        game_info.append(f"{col}: {value}")
                
                if game_info:
                    print(f"📈 Sample game: {' | '.join(game_info)}")
            
            # Save sample data
            filename = f"/Users/evgen/projects/ek_nfl_fantasy/dev/data/sample_game_log_{player_name.split('(')[0].strip().replace(' ', '_')}.csv"
            game_log.to_csv(filename, index=False)
            print(f"📁 Saved: {filename}")
            
            results[player_name] = {
                'success': True,
                'games': len(game_log),
                'columns': list(game_log.columns),
                'file': filename
            }
        else:
            print(f"❌ Failed to extract game log")
            results[player_name] = {
                'success': False
            }
        
        time.sleep(1)  # Be respectful between requests
    
    # Summary
    print(f"\n📋 Game Log Extraction Summary:")
    print("=" * 40)
    
    successful = sum(1 for result in results.values() if result['success'])
    total_games = sum(result.get('games', 0) for result in results.values())
    
    print(f"✅ Successful extractions: {successful}/{len(test_players)}")
    print(f"📊 Total games extracted: {total_games}")
    
    for player, result in results.items():
        status = "✅" if result['success'] else "❌"
        games = f" ({result['games']} games)" if result['success'] else ""
        print(f"{status} {player}{games}")
    
    return results

def save_extraction_test_results(results):
    """Save test results for analysis"""
    
    summary = {
        'test_date': datetime.now().isoformat(),
        'total_players_tested': len(results),
        'successful_extractions': sum(1 for r in results.values() if r['success']),
        'total_games_extracted': sum(r.get('games', 0) for r in results.values()),
        'results': results
    }
    
    summary_file = "/Users/evgen/projects/ek_nfl_fantasy/dev/data/game_log_test_results.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"📁 Test results saved: {summary_file}")
    return summary

def main():
    """Main testing function"""
    print("Phase 1.2 Step 3: Game Log Extraction Testing")
    print("=" * 50)
    
    results = test_known_players()
    summary = save_extraction_test_results(results)
    
    print(f"\n🎉 Testing Complete!")
    print(f"Game log extraction is {'working' if summary['successful_extractions'] > 0 else 'not working'}")
    
    if summary['successful_extractions'] > 0:
        print(f"✅ Ready to build full extraction pipeline")
    else:
        print(f"❌ Need to debug game log extraction further")
    
    return results

if __name__ == "__main__":
    main()