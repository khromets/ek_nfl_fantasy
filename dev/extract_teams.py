#!/usr/bin/env python3
"""
Extract and populate NFL teams data
"""
import sys
import logging
from pathlib import Path

# Add the data extraction modules to path
sys.path.append(str(Path(__file__).parent / "data_extraction"))

from data_extraction.core.database import initialize_database, DatabaseManager
from data_extraction.extractors.teams_extractor import TeamsExtractor
from data_extraction.core.data_validator import DataValidator

def main():
    """Extract teams and populate database"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting NFL teams extraction")
        
        # Initialize database
        logger.info("Initializing database")
        db = initialize_database()
        
        # Extract teams
        logger.info("Extracting teams data")
        extractor = TeamsExtractor()
        teams = extractor.extract_and_validate_teams()
        
        # Insert teams into database
        logger.info("Inserting teams into database")
        inserted_count = 0
        
        for team in teams:
            try:
                team_id = db.insert_data('teams', {
                    'team_code': team['team_code'],
                    'team_name': team['team_name'],
                    'conference': team['conference'],
                    'division': team['division']
                })
                inserted_count += 1
                logger.debug(f"Inserted team {team['team_code']} with ID {team_id}")
                
            except Exception as e:
                logger.error(f"Failed to insert team {team['team_code']}: {e}")
        
        # Validate database content
        logger.info("Validating teams in database")
        teams_in_db = db.query("SELECT team_code, team_name, conference, division FROM teams ORDER BY team_code")
        teams_data = [dict(row) for row in teams_in_db]
        
        validator = DataValidator()
        is_valid = validator.validate_team_data(teams_data)
        validator.log_validation_results()
        
        if is_valid:
            logger.info(f"✅ Successfully extracted and stored {inserted_count} NFL teams")
            
            # Show summary by conference/division
            summary = {}
            for team in teams_data:
                conf_div = f"{team['conference']} {team['division']}"
                if conf_div not in summary:
                    summary[conf_div] = 0
                summary[conf_div] += 1
            
            logger.info("Teams by conference/division:")
            for conf_div, count in sorted(summary.items()):
                logger.info(f"  {conf_div}: {count} teams")
        else:
            logger.error("❌ Team data validation failed")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Failed to extract teams: {e}")
        return 1
    
    finally:
        if 'db' in locals():
            db.disconnect()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)