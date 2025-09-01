#!/usr/bin/env python3
"""
Test pro-football-reference-web-scraper package
"""
import sys
import logging
import pandas as pd

def test_pfr_scraper():
    """Test the pro-football-reference-web-scraper package"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Test player game log
        logger.info("=== Testing Player Game Log ===")
        
        from pro_football_reference_web_scraper import player_game_log as p
        
        # Test with a well-known player
        logger.info("Testing with Josh Allen (QB) 2022...")
        game_log = p.get_player_game_log(player='Josh Allen', position='QB', season=2022)
        
        if isinstance(game_log, pd.DataFrame) and not game_log.empty:
            logger.info(f"✅ Successfully extracted {len(game_log)} games for Josh Allen")
            logger.info(f"Columns: {list(game_log.columns)}")
            logger.info("Sample data:")
            print(game_log.head(2))
            
            # Test with another position
            logger.info("\nTesting with Derrick Henry (RB) 2022...")
            rb_log = p.get_player_game_log(player='Derrick Henry', position='RB', season=2022)
            
            if isinstance(rb_log, pd.DataFrame) and not rb_log.empty:
                logger.info(f"✅ Successfully extracted {len(rb_log)} games for Derrick Henry")
                logger.info(f"RB Columns: {list(rb_log.columns)}")
                print(rb_log.head(2))
            else:
                logger.warning("❌ Failed to get Derrick Henry data")
            
        else:
            logger.error("❌ Failed to get Josh Allen data")
            return False
        
        # Test team game log
        logger.info("\n=== Testing Team Game Log ===")
        
        from pro_football_reference_web_scraper import team_game_log as t
        
        logger.info("Testing with Kansas City Chiefs 2022...")
        team_log = t.get_team_game_log(team='Kansas City Chiefs', season=2022)
        
        if isinstance(team_log, pd.DataFrame) and not team_log.empty:
            logger.info(f"✅ Successfully extracted {len(team_log)} games for KC Chiefs")
            logger.info(f"Team Columns: {list(team_log.columns)}")
            print(team_log.head(2))
        else:
            logger.warning("❌ Failed to get Chiefs team data")
        
        logger.info("\n🎉 Pro Football Reference scraper is working!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_pfr_scraper()
    sys.exit(0 if success else 1)