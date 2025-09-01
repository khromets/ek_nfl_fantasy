#!/usr/bin/env python3
"""
Test ESPN API for NFL data access
Tests what data is available through ESPN's public API
"""

import requests
import json
from datetime import datetime

def test_espn_teams():
    """Test ESPN teams API"""
    print("Testing ESPN Teams API:")
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            teams = data.get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', [])
            print(f"✓ Found {len(teams)} NFL teams")
            
            # Show sample team data
            if teams:
                sample_team = teams[0]['team']
                print(f"  Sample team: {sample_team['displayName']} ({sample_team['abbreviation']})")
                print(f"  Available fields: {list(sample_team.keys())}")
            
            return teams
        else:
            print(f"✗ API returned status code: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def test_espn_scoreboard():
    """Test ESPN scoreboard for recent games"""
    print("\nTesting ESPN Scoreboard API:")
    try:
        # Get recent games (current week)
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            events = data.get('events', [])
            print(f"✓ Found {len(events)} recent games")
            
            if events:
                sample_game = events[0]
                competitions = sample_game.get('competitions', [])
                if competitions:
                    competitors = competitions[0].get('competitors', [])
                    if len(competitors) >= 2:
                        team1 = competitors[0]['team']['displayName']
                        team2 = competitors[1]['team']['displayName']
                        print(f"  Sample game: {team1} vs {team2}")
                        print(f"  Game fields: {list(sample_game.keys())}")
            
            return events
        else:
            print(f"✗ API returned status code: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def test_espn_athletes():
    """Test ESPN athletes/players API"""
    print("\nTesting ESPN Athletes API:")
    try:
        # Try to get players for a specific team (e.g., Dallas Cowboys)
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/6/athletes"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            athletes = data.get('athletes', [])
            print(f"✓ Found {len(athletes)} athletes for team")
            
            if athletes:
                sample_athlete = athletes[0]
                print(f"  Sample player: {sample_athlete.get('displayName', 'Unknown')}")
                print(f"  Position: {sample_athlete.get('position', {}).get('displayName', 'Unknown')}")
                print(f"  Available fields: {list(sample_athlete.keys())}")
            
            return athletes
        else:
            print(f"✗ API returned status code: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def test_espn_player_stats():
    """Test if we can get player statistics"""
    print("\nTesting ESPN Player Statistics:")
    try:
        # Try to get stats for a specific player (this might not work without proper endpoints)
        # Let's try the general stats endpoint
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/statistics"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print("✓ Stats endpoint accessible")
            print(f"  Available categories: {list(data.keys())}")
            return data
        else:
            print(f"✗ Stats API returned status code: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def main():
    """Main test function"""
    print("ESPN API Test for NFL Data")
    print("=" * 50)
    
    # Test different ESPN endpoints
    teams = test_espn_teams()
    games = test_espn_scoreboard()
    athletes = test_espn_athletes()
    stats = test_espn_player_stats()
    
    print("\n" + "=" * 50)
    print("SUMMARY:")
    
    if teams:
        print("✓ Team data: Available")
    else:
        print("✗ Team data: Not available")
        
    if games:
        print("✓ Game data: Available")
    else:
        print("✗ Game data: Not available")
        
    if athletes:
        print("✓ Player roster: Available")
    else:
        print("✗ Player roster: Not available")
        
    if stats:
        print("✓ Statistics: Available")
    else:
        print("✗ Statistics: Limited/Not available")
    
    print("\nNEXT STEPS:")
    print("1. ESPN API provides basic team and game data")
    print("2. Player statistics may require different endpoints or scraping")
    print("3. Consider Pro Football Reference for detailed historical stats")
    print("4. Alternative: NFL's official API or fantasy sports APIs")
    
    print(f"\nTest completed at: {datetime.now()}")

if __name__ == "__main__":
    main()