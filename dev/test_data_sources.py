#!/usr/bin/env python3
"""
Test script for NFL data source accessibility and structure
Phase 1.2 - Step 1: Data Source Selection & Testing
"""

import requests
from bs4 import BeautifulSoup
import time
import json
from urllib.parse import urljoin, urlparse

class DataSourceTester:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.results = {}

    def test_nfl_com_stats(self):
        """Test NFL.com statistics page accessibility and structure"""
        print("Testing NFL.com statistics pages...")
        
        base_urls = {
            'passing': 'https://www.nfl.com/stats/player-stats/',
            'rushing': 'https://www.nfl.com/stats/player-stats/category/rushing/2024/reg/all/rushingyards/desc',
            'receiving': 'https://www.nfl.com/stats/player-stats/category/receiving/2024/reg/all/receivingreceptions/desc'
        }
        
        nfl_results = {}
        
        for stat_type, url in base_urls.items():
            try:
                print(f"  Testing {stat_type} stats...")
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for data tables
                    tables = soup.find_all('table')
                    data_elements = soup.find_all(['div', 'section'], class_=lambda x: x and 'stats' in str(x).lower())
                    
                    nfl_results[stat_type] = {
                        'status': 'accessible',
                        'status_code': response.status_code,
                        'tables_found': len(tables),
                        'data_elements': len(data_elements),
                        'page_title': soup.title.text if soup.title else 'No title'
                    }
                    
                    # Look for player links or IDs
                    player_links = soup.find_all('a', href=lambda x: x and '/players/' in str(x))
                    nfl_results[stat_type]['player_links'] = len(player_links)
                    
                else:
                    nfl_results[stat_type] = {
                        'status': 'inaccessible',
                        'status_code': response.status_code,
                        'error': f'HTTP {response.status_code}'
                    }
                    
                time.sleep(1)  # Respectful delay
                
            except Exception as e:
                nfl_results[stat_type] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        self.results['nfl_com'] = nfl_results
        return nfl_results

    def test_pro_football_reference(self):
        """Test Pro Football Reference accessibility and structure"""
        print("Testing Pro Football Reference...")
        
        test_urls = {
            'season_rushing': 'https://www.pro-football-reference.com/years/2024/rushing.htm',
            'season_passing': 'https://www.pro-football-reference.com/years/2024/passing.htm',
            'sample_game_log': 'https://www.pro-football-reference.com/players/P/PeniMi00/gamelog/2024/'
        }
        
        pfr_results = {}
        
        for test_type, url in test_urls.items():
            try:
                print(f"  Testing {test_type}...")
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for data tables (PFR uses specific table structure)
                    stats_tables = soup.find_all('table', {'class': 'stats_table'})
                    all_tables = soup.find_all('table')
                    
                    pfr_results[test_type] = {
                        'status': 'accessible',
                        'status_code': response.status_code,
                        'stats_tables': len(stats_tables),
                        'all_tables': len(all_tables),
                        'page_title': soup.title.text if soup.title else 'No title'
                    }
                    
                    # Look for player links
                    player_links = soup.find_all('a', href=lambda x: x and '/players/' in str(x))
                    pfr_results[test_type]['player_links'] = len(player_links)
                    
                    # Check for specific data indicators
                    if stats_tables:
                        first_table = stats_tables[0]
                        headers = [th.text.strip() for th in first_table.find_all('th')]
                        pfr_results[test_type]['sample_headers'] = headers[:10]  # First 10 headers
                    
                else:
                    pfr_results[test_type] = {
                        'status': 'inaccessible',
                        'status_code': response.status_code
                    }
                    
                time.sleep(1)  # Respectful delay
                
            except Exception as e:
                pfr_results[test_type] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        self.results['pro_football_reference'] = pfr_results
        return pfr_results

    def test_espn_stats(self):
        """Test ESPN statistics accessibility and structure"""
        print("Testing ESPN statistics...")
        
        test_urls = {
            'rushing': 'https://www.espn.com/nfl/stats/player/_/stat/rushing',
            'passing': 'https://www.espn.com/nfl/stats/player/_/stat/passing',
            'receiving': 'https://www.espn.com/nfl/stats/player/_/stat/receiving'
        }
        
        espn_results = {}
        
        for stat_type, url in test_urls.items():
            try:
                print(f"  Testing {stat_type} stats...")
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for data tables
                    tables = soup.find_all('table')
                    stats_tables = soup.find_all('table', class_=lambda x: x and 'stats' in str(x).lower() if x else False)
                    
                    espn_results[stat_type] = {
                        'status': 'accessible',
                        'status_code': response.status_code,
                        'all_tables': len(tables),
                        'stats_tables': len(stats_tables),
                        'page_title': soup.title.text if soup.title else 'No title'
                    }
                    
                    # Look for player links or data
                    player_links = soup.find_all('a', href=lambda x: x and ('player' in str(x).lower() or 'id/' in str(x)))
                    espn_results[stat_type]['player_elements'] = len(player_links)
                    
                else:
                    espn_results[stat_type] = {
                        'status': 'inaccessible',
                        'status_code': response.status_code
                    }
                    
                time.sleep(1)  # Respectful delay
                
            except Exception as e:
                espn_results[stat_type] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        self.results['espn'] = espn_results
        return espn_results

    def generate_report(self):
        """Generate comprehensive test report"""
        print("\n" + "="*60)
        print("DATA SOURCE TESTING REPORT")
        print("="*60)
        
        for source, data in self.results.items():
            print(f"\n{source.upper()}:")
            print("-" * 40)
            
            for test_type, results in data.items():
                status = results.get('status', 'unknown')
                print(f"  {test_type}: {status.upper()}")
                
                if status == 'accessible':
                    print(f"    - Tables found: {results.get('all_tables', 0)} ({results.get('stats_tables', 0)} stats tables)")
                    print(f"    - Player links: {results.get('player_links', 0)}")
                    if 'sample_headers' in results:
                        print(f"    - Sample headers: {', '.join(results['sample_headers'])}")
                elif status in ['inaccessible', 'error']:
                    print(f"    - Error: {results.get('error', 'Unknown error')}")
        
        # Generate recommendations
        print(f"\n{'RECOMMENDATIONS'}")
        print("-" * 40)
        
        # Analyze results and provide recommendations
        accessible_sources = []
        for source, data in self.results.items():
            accessible_tests = sum(1 for test_results in data.values() 
                                 if test_results.get('status') == 'accessible')
            total_tests = len(data)
            
            if accessible_tests > 0:
                accessible_sources.append((source, accessible_tests, total_tests))
        
        accessible_sources.sort(key=lambda x: x[1], reverse=True)
        
        if accessible_sources:
            primary = accessible_sources[0]
            print(f"Primary recommendation: {primary[0].upper()}")
            print(f"  - {primary[1]}/{primary[2]} tests successful")
            
            if len(accessible_sources) > 1:
                secondary = accessible_sources[1]
                print(f"Secondary recommendation: {secondary[0].upper()}")
                print(f"  - {secondary[1]}/{secondary[2]} tests successful")
        else:
            print("No sources fully accessible - may need different approach")

    def save_results(self, filename='data_source_test_results.json'):
        """Save results to JSON file"""
        filepath = f"/Users/evgen/projects/ek_nfl_fantasy/dev/{filename}"
        with open(filepath, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to: {filepath}")

def main():
    """Run all data source tests"""
    print("NFL Data Source Testing - Phase 1.2 Step 1")
    print("=" * 50)
    
    tester = DataSourceTester()
    
    # Test all sources
    tester.test_nfl_com_stats()
    tester.test_pro_football_reference()
    tester.test_espn_stats()
    
    # Generate report
    tester.generate_report()
    
    # Save results
    tester.save_results()
    
    print("\n" + "="*50)
    print("Testing completed!")

if __name__ == "__main__":
    main()