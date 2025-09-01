#!/usr/bin/env python3
"""
Test script for nfl-data-py package and alternatives
Tests basic functionality and data availability for 2020-2024 seasons
"""

import sys
from datetime import datetime

def test_pandas_import():
    """Test if pandas can be imported"""
    try:
        import pandas as pd
        print(f"✓ pandas imported successfully (version: {pd.__version__})")
        return pd
    except ImportError as e:
        print(f"✗ Failed to import pandas: {e}")
        return None

def test_nfl_data_import():
    """Test if nfl_data_py can be imported"""
    try:
        import nfl_data_py as nfl
        print("✓ nfl_data_py imported successfully")
        return nfl
    except ImportError as e:
        print(f"✗ Failed to import nfl_data_py: {e}")
        print("  This could be due to installation issues")
        return None

def test_alternative_apis():
    """Test alternative NFL data sources"""
    print("\nTesting alternative NFL data sources:")
    
    # Test requests for web scraping
    try:
        import requests
        print("✓ requests library available for web scraping")
        
        # Test a simple ESPN API call
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ ESPN API accessible - found {len(data.get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', []))} teams")
        else:
            print(f"✗ ESPN API returned status code: {response.status_code}")
            
    except ImportError:
        print("✗ requests library not available")
    except Exception as e:
        print(f"✗ Error testing ESPN API: {e}")
    
    # Test if urllib is available for basic web requests
    try:
        import urllib.request
        import json
        print("✓ urllib available for basic web requests")
    except ImportError:
        print("✗ urllib not available")

def test_player_data(nfl, years=[2020, 2021, 2022, 2023, 2024]):
    """Test player roster data availability"""
    print(f"\nTesting player roster data for years: {years}")
    
    try:
        rosters = nfl.import_rosters(years)
        print(f"✓ Player rosters loaded: {len(rosters)} records")
        print(f"  Columns: {list(rosters.columns)}")
        print(f"  Sample positions: {rosters['position'].value_counts().head()}")
        return rosters
    except Exception as e:
        print(f"✗ Failed to load player rosters: {e}")
        return None

def test_passing_stats(nfl, years=[2020, 2021, 2022, 2023]):
    """Test QB passing statistics"""
    print(f"\nTesting passing stats for years: {years}")
    
    try:
        passing = nfl.import_seasonal_data(years, s_type='REG', columns=[
            'player_id', 'player_name', 'position', 'team', 'season',
            'passing_yards', 'passing_tds', 'interceptions', 'passing_2pt_conversions'
        ])
        qb_data = passing[passing['position'] == 'QB']
        print(f"✓ QB passing stats loaded: {len(qb_data)} records")
        if len(qb_data) > 0:
            print(f"  Sample QB: {qb_data.iloc[0]['player_name']} - {qb_data.iloc[0]['passing_yards']} yards")
        return qb_data
    except Exception as e:
        print(f"✗ Failed to load passing stats: {e}")
        return None

def test_rushing_receiving_stats(nfl, years=[2020, 2021, 2022, 2023]):
    """Test RB/WR/TE rushing and receiving statistics"""
    print(f"\nTesting rushing/receiving stats for years: {years}")
    
    try:
        stats = nfl.import_seasonal_data(years, s_type='REG', columns=[
            'player_id', 'player_name', 'position', 'team', 'season',
            'rushing_yards', 'rushing_tds', 'receiving_yards', 'receiving_tds', 
            'receptions', 'fumbles_lost'
        ])
        skill_positions = stats[stats['position'].isin(['RB', 'WR', 'TE'])]
        print(f"✓ Skill position stats loaded: {len(skill_positions)} records")
        if len(skill_positions) > 0:
            print(f"  Positions breakdown: {skill_positions['position'].value_counts().to_dict()}")
        return skill_positions
    except Exception as e:
        print(f"✗ Failed to load rushing/receiving stats: {e}")
        return None

def test_weekly_data(nfl, years=[2023]):
    """Test weekly game-by-game data availability"""
    print(f"\nTesting weekly data for years: {years}")
    
    try:
        weekly = nfl.import_weekly_data(years, columns=[
            'player_id', 'player_name', 'position', 'team', 'week', 'season',
            'passing_yards', 'passing_tds', 'rushing_yards', 'rushing_tds',
            'receiving_yards', 'receiving_tds', 'receptions'
        ])
        print(f"✓ Weekly data loaded: {len(weekly)} records")
        if len(weekly) > 0:
            print(f"  Weeks available: {sorted(weekly['week'].unique())}")
            print(f"  Sample record: {weekly.iloc[0]['player_name']} Week {weekly.iloc[0]['week']}")
        return weekly
    except Exception as e:
        print(f"✗ Failed to load weekly data: {e}")
        return None

def test_pbp_data(nfl, years=[2023]):
    """Test play-by-play data for defensive stats"""
    print(f"\nTesting play-by-play data for years: {years}")
    
    try:
        # Test with a small sample first
        pbp = nfl.import_pbp_data(years, columns=[
            'play_id', 'game_id', 'week', 'season', 'play_type',
            'interception', 'fumble', 'sack', 'tackle_for_loss'
        ])
        print(f"✓ Play-by-play data loaded: {len(pbp)} records")
        if len(pbp) > 0:
            print(f"  Play types: {pbp['play_type'].value_counts().head()}")
        return pbp
    except Exception as e:
        print(f"✗ Failed to load play-by-play data: {e}")
        return None

def main():
    """Main test function"""
    print("NFL Data Package Test")
    print("=" * 50)
    
    # Test pandas first
    pd = test_pandas_import()
    
    # Test nfl_data_py import
    nfl = test_nfl_data_import()
    
    # Test alternative data sources
    test_alternative_apis()
    
    # If nfl_data_py is available, test its functionality
    if nfl and pd:
        print("\n" + "=" * 30)
        print("Testing nfl_data_py functionality:")
        test_player_data(nfl)
        test_passing_stats(nfl)
        test_rushing_receiving_stats(nfl)
        test_weekly_data(nfl)
        test_pbp_data(nfl)
    else:
        print("\n" + "=" * 30)
        print("RECOMMENDATIONS:")
        print("1. Install pandas with: pip install 'pandas>=1.3.0,<2.0'")
        print("2. Install nfl_data_py with: pip install nfl_data_py")
        print("3. Alternative: Use ESPN API or Pro Football Reference scraping")
        print("4. Consider using nfl_data_py in a conda environment")
    
    print("\n" + "=" * 50)
    print("Test completed!")
    print(f"Test run at: {datetime.now()}")

if __name__ == "__main__":
    main()