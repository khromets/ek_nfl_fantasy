#!/usr/bin/env python3
"""
Test the data extraction pipeline with a small sample
"""
import sys
import logging
from pathlib import Path

# Add the data extraction modules to path
sys.path.append(str(Path(__file__).parent / "data_extraction"))

from data_extraction.core.database import initialize_database
from data_extraction.extractors.teams_extractor import TeamsExtractor
from data_extraction.extractors.games_extractor import GamesExtractor
from data_extraction.extractors.players_extractor import PlayersExtractor

def test_teams_extraction():
    """Test team extraction"""
    print("=== Testing Teams Extraction ===")
    
    try:
        extractor = TeamsExtractor()
        teams = extractor.extract_and_validate_teams()
        
        print(f"✅ Extracted {len(teams)} teams")
        print("Sample teams:")
        for team in teams[:3]:
            print(f"  {team['team_code']}: {team['team_name']} ({team['conference']} {team['division']})")
        
        return True, teams
        
    except Exception as e:
        print(f"❌ Teams extraction failed: {e}")
        return False, []

def test_games_extraction(team_mappings):
    """Test games extraction for one season"""
    print("\n=== Testing Games Extraction ===")
    
    try:
        extractor = GamesExtractor()
        extractor.set_team_mappings(team_mappings)
        
        # Test with 2024 season only
        games = extractor.extract_games_for_season(2024)
        
        print(f"✅ Extracted {len(games)} games for 2024 season")
        
        if games:
            print("Sample games:")
            for game in games[:3]:
                home_code = next(code for code, id in team_mappings.items() if id == game['home_team_id'])
                away_code = next(code for code, id in team_mappings.items() if id == game['away_team_id'])
                print(f"  Week {game['week']}: {away_code} @ {home_code} ({game['game_date']})")
        
        return True, games
        
    except Exception as e:
        print(f"❌ Games extraction failed: {e}")
        return False, []

def test_players_extraction(team_mappings):
    """Test player extraction for one team"""
    print("\n=== Testing Players Extraction ===")
    
    try:
        extractor = PlayersExtractor()
        extractor.set_team_mappings(team_mappings)
        
        # Test with just Kansas City Chiefs for 2024
        players = extractor.extract_team_roster('KC', 2024)
        
        print(f"✅ Extracted {len(players)} players from KC 2024 roster")
        
        if players:
            print("Sample players:")
            for player in players[:5]:
                height = f"{player.get('height_inches', 'N/A')}\"" if player.get('height_inches') else "N/A"
                weight = f"{player.get('weight_lbs', 'N/A')} lbs" if player.get('weight_lbs') else "N/A"
                print(f"  {player['name']} ({player['position']}) - {height}, {weight}")
        
        return True, players
        
    except Exception as e:
        print(f"❌ Players extraction failed: {e}")
        return False, []

def test_database_operations():
    """Test database initialization and basic operations"""
    print("\n=== Testing Database Operations ===")
    
    try:
        # Initialize database
        db = initialize_database()
        
        # Test basic queries
        tables = db.query("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [table['name'] for table in tables]
        
        expected_tables = ['teams', 'players', 'games', 'passing_stats', 'fantasy_points']
        found_tables = [table for table in expected_tables if table in table_names]
        
        print(f"✅ Database initialized with {len(table_names)} tables")
        print(f"Found expected tables: {found_tables}")
        
        db.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🏈 NFL Data Extraction Pipeline Test\n")
    
    # Setup logging
    logging.basicConfig(level=logging.WARNING)  # Reduce log noise for testing
    
    all_passed = True
    
    # Test database
    db_success = test_database_operations()
    all_passed &= db_success
    
    if not db_success:
        print("❌ Cannot continue without database")
        return 1
    
    # Test teams extraction
    teams_success, teams = test_teams_extraction()
    all_passed &= teams_success
    
    if not teams_success:
        print("❌ Cannot continue without teams")
        return 1
    
    # Create team mappings for testing
    team_mappings = {team['team_code']: i+1 for i, team in enumerate(teams)}
    
    # Test games extraction
    games_success, games = test_games_extraction(team_mappings)
    all_passed &= games_success
    
    # Test players extraction
    players_success, players = test_players_extraction(team_mappings)
    all_passed &= players_success
    
    # Summary
    print(f"\n{'='*50}")
    if all_passed:
        print("🎉 All tests passed! The extraction pipeline is working.")
        print("\nNext steps:")
        print("1. Run 'python extract_all_data.py' to extract all data")
        print("2. Build statistical data extraction scripts")
        print("3. Implement fantasy points calculation")
    else:
        print("❌ Some tests failed. Check the errors above.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)