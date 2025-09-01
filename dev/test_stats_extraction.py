#!/usr/bin/env python3
"""
Test statistical data extraction
"""
import sys
import logging
from pathlib import Path

# Add the data extraction modules to path
sys.path.append(str(Path(__file__).parent / "data_extraction"))

from data_extraction.core.database import DatabaseManager
from data_extraction.extractors.stats_extractor import StatsExtractor

def test_stats_extraction():
    """Test statistical data extraction for a few games"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Connect to database
        db = DatabaseManager()
        db.connect()
        
        # Get team mappings
        teams_data = db.query("SELECT team_id, team_code FROM teams")
        team_mappings = {row['team_code']: row['team_id'] for row in teams_data}
        
        logger.info(f"Got {len(team_mappings)} team mappings")
        
        # Get a few games to test with
        test_games = db.query("""
            SELECT game_id, nfl_game_id, season, week, home_team_id, away_team_id 
            FROM games 
            WHERE season >= 2020 
            ORDER BY season DESC, week ASC 
            LIMIT 3
        """)
        
        if not test_games:
            logger.error("No games found in database")
            return False
        
        logger.info(f"Testing with {len(test_games)} games:")
        for game in test_games:
            logger.info(f"  Game {game['nfl_game_id']}: Season {game['season']}, Week {game['week']}")
        
        # Initialize stats extractor
        extractor = StatsExtractor()
        extractor.set_mappings(team_mappings)
        
        # Test extraction for first game
        test_game = test_games[0]
        logger.info(f"\n=== Testing game {test_game['nfl_game_id']} ===")
        
        stats = extractor.extract_game_stats(
            test_game['nfl_game_id'],
            test_game['season'],
            test_game['week']
        )
        
        # Show results
        total_stats = sum(len(category_stats) for category_stats in stats.values())
        logger.info(f"Extracted {total_stats} total stat records:")
        
        for category, category_stats in stats.items():
            logger.info(f"  {category}: {len(category_stats)} records")
            
            # Show sample stats
            if category_stats:
                sample = category_stats[0]
                logger.info(f"    Sample {category} stat: {sample}")
        
        if total_stats > 0:
            logger.info("✅ Stats extraction is working!")
            return True
        else:
            logger.warning("⚠️  No stats extracted - API format may have changed")
            return False
        
    except Exception as e:
        logger.error(f"❌ Stats extraction test failed: {e}")
        return False
    
    finally:
        if 'db' in locals():
            db.disconnect()

if __name__ == "__main__":
    success = test_stats_extraction()
    sys.exit(0 if success else 1)