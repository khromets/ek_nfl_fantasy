#!/usr/bin/env python3
"""
Test player extraction with specific season
"""
import sys
import logging
from pathlib import Path

# Add the data extraction modules to path
sys.path.append(str(Path(__file__).parent / "data_extraction"))

from data_extraction.core.database import DatabaseManager
from data_extraction.extractors.players_extractor import PlayersExtractor

def test_player_extraction():
    """Test player extraction for a specific team and season"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Get team mappings from database
        db = DatabaseManager()
        db.connect()
        
        team_mappings = {}
        teams_in_db = db.query("SELECT team_id, team_code FROM teams")
        for row in teams_in_db:
            team_mappings[row['team_code']] = row['team_id']
        
        logger.info(f"Got team mappings for {len(team_mappings)} teams")
        
        # Test player extraction
        extractor = PlayersExtractor()
        extractor.set_team_mappings(team_mappings)
        
        # Test with Kansas City Chiefs 2021
        logger.info("Testing with KC 2021...")
        players = extractor.extract_team_roster('KC', 2021)
        
        if players:
            logger.info(f"✅ Successfully extracted {len(players)} players from KC 2021:")
            for player in players[:5]:
                logger.info(f"  {player['name']} ({player['position']}) - {player.get('height_inches', 'N/A')}\" {player.get('weight_lbs', 'N/A')}lbs")
            
            # Try inserting into database
            inserted = 0
            for player in players:
                try:
                    # Check if player already exists
                    existing = db.query(
                        "SELECT player_id FROM players WHERE name = ? AND position = ?",
                        (player['name'], player['position'])
                    )
                    
                    if not existing:
                        db.insert_data('players', {
                            'name': player['name'],
                            'position': player['position'],
                            'team_id': player['team_id'],
                            'height_inches': player.get('height_inches'),
                            'weight_lbs': player.get('weight_lbs'),
                            'college': player.get('college')
                        })
                        inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert {player['name']}: {e}")
            
            logger.info(f"✅ Inserted {inserted} new players into database")
            
            # Test with a few more teams
            for team_code in ['SF', 'GB', 'NE']:
                logger.info(f"Testing {team_code} 2021...")
                team_players = extractor.extract_team_roster(team_code, 2021)
                logger.info(f"  Extracted {len(team_players)} players from {team_code}")
                
        else:
            logger.error("❌ No players extracted from KC 2021")
            return False
        
        db.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_player_extraction()
    if success:
        print("\n🎉 Player extraction test passed!")
    else:
        print("\n❌ Player extraction test failed!")
    sys.exit(0 if success else 1)