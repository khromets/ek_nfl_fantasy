#!/usr/bin/env python3
"""
Master script to extract all NFL data (teams, games, players) and populate database
"""
import sys
import logging
from pathlib import Path

# Add the data extraction modules to path
sys.path.append(str(Path(__file__).parent / "data_extraction"))

from data_extraction.core.database import initialize_database, DatabaseManager
from data_extraction.extractors.teams_extractor import TeamsExtractor
from data_extraction.extractors.games_extractor import GamesExtractor
from data_extraction.extractors.players_extractor import PlayersExtractor
from data_extraction.core.data_validator import DataValidator, validate_data_completeness
from data_extraction.core.config import SEASONS

def setup_logging():
    """Setup comprehensive logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data_extraction.log')
        ]
    )
    return logging.getLogger(__name__)

def extract_and_store_teams(db: DatabaseManager, logger: logging.Logger) -> dict:
    """Extract teams and store in database"""
    logger.info("=== EXTRACTING TEAMS ===")
    
    try:
        extractor = TeamsExtractor()
        teams = extractor.extract_and_validate_teams()
        
        # Insert teams
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
            except Exception as e:
                logger.error(f"Failed to insert team {team['team_code']}: {e}")
        
        # Create team mappings for other extractors
        team_mappings = {}
        teams_in_db = db.query("SELECT team_id, team_code FROM teams")
        for row in teams_in_db:
            team_mappings[row['team_code']] = row['team_id']
        
        logger.info(f"✅ Successfully stored {inserted_count} teams")
        return {'success': True, 'count': inserted_count, 'mappings': team_mappings}
        
    except Exception as e:
        logger.error(f"❌ Failed to extract teams: {e}")
        return {'success': False, 'error': str(e), 'mappings': {}}

def extract_and_store_games(db: DatabaseManager, team_mappings: dict, 
                          logger: logging.Logger) -> dict:
    """Extract games and store in database"""
    logger.info("=== EXTRACTING GAMES ===")
    
    try:
        extractor = GamesExtractor()
        extractor.set_team_mappings(team_mappings)
        
        total_inserted = 0
        
        for season in SEASONS:
            logger.info(f"Extracting games for {season} season")
            
            games = extractor.extract_games_for_season(season)
            
            # Insert games
            season_inserted = 0
            for game in games:
                try:
                    db.insert_data('games', {
                        'nfl_game_id': game['nfl_game_id'],
                        'season': game['season'],
                        'week': game['week'],
                        'game_date': game['game_date'],
                        'home_team_id': game['home_team_id'],
                        'away_team_id': game['away_team_id'],
                        'home_score': game.get('home_score'),
                        'away_score': game.get('away_score'),
                        'game_type': game.get('game_type', 'REG')
                    })
                    season_inserted += 1
                except Exception as e:
                    logger.error(f"Failed to insert game {game['nfl_game_id']}: {e}")
            
            total_inserted += season_inserted
            logger.info(f"Stored {season_inserted} games for {season} season")
        
        logger.info(f"✅ Successfully stored {total_inserted} total games")
        return {'success': True, 'count': total_inserted}
        
    except Exception as e:
        logger.error(f"❌ Failed to extract games: {e}")
        return {'success': False, 'error': str(e), 'count': 0}

def extract_and_store_players(db: DatabaseManager, team_mappings: dict, 
                            logger: logging.Logger, seasons: list = None) -> dict:
    """Extract players and store in database"""
    logger.info("=== EXTRACTING PLAYERS ===")
    
    if seasons is None:
        seasons = [SEASONS[-1]]  # Start with most recent season only
    
    try:
        extractor = PlayersExtractor()
        extractor.set_team_mappings(team_mappings)
        
        total_inserted = 0
        
        for season in seasons:
            logger.info(f"Extracting player rosters for {season} season")
            
            players = extractor.extract_all_rosters_for_season(season)
            
            # Insert players
            season_inserted = 0
            for player in players:
                try:
                    # Check if player already exists (by name and position)
                    existing = db.query(
                        "SELECT player_id FROM players WHERE name = ? AND position = ?",
                        (player['name'], player['position'])
                    )
                    
                    if existing:
                        # Update existing player's team if needed
                        player_id = existing[0]['player_id']
                        db.query(
                            "UPDATE players SET team_id = ?, updated_at = CURRENT_TIMESTAMP WHERE player_id = ?",
                            (player['team_id'], player_id)
                        )
                    else:
                        # Insert new player
                        db.insert_data('players', {
                            'name': player['name'],
                            'position': player['position'],
                            'team_id': player['team_id'],
                            'height_inches': player.get('height_inches'),
                            'weight_lbs': player.get('weight_lbs'),
                            'college': player.get('college')
                        })
                        season_inserted += 1
                        
                except Exception as e:
                    logger.error(f"Failed to insert player {player['name']}: {e}")
            
            total_inserted += season_inserted
            logger.info(f"Stored {season_inserted} new players for {season} season")
        
        logger.info(f"✅ Successfully stored {total_inserted} total players")
        return {'success': True, 'count': total_inserted}
        
    except Exception as e:
        logger.error(f"❌ Failed to extract players: {e}")
        return {'success': False, 'error': str(e), 'count': 0}

def validate_database_completeness(db: DatabaseManager, logger: logging.Logger):
    """Validate completeness of extracted data"""
    logger.info("=== VALIDATING DATA COMPLETENESS ===")
    
    try:
        report = validate_data_completeness(db, SEASONS)
        
        # Log summary
        teams_complete = report.get('teams', {}).get('complete', False)
        logger.info(f"Teams: {report.get('teams', {}).get('count', 0)}/32 {'✅' if teams_complete else '❌'}")
        
        for season in SEASONS:
            games_info = report.get('games', {}).get(season, {})
            games_complete = games_info.get('complete', False)
            games_count = games_info.get('count', 0)
            logger.info(f"Games {season}: {games_count} {'✅' if games_complete else '❌'}")
        
        players_count = report.get('players', {}).get('count', 0)
        logger.info(f"Players: {players_count}")
        
        stats_info = report.get('statistics', {})
        for table, info in stats_info.items():
            count = info.get('count', 0)
            logger.info(f"{table}: {count} records")
        
        return report
        
    except Exception as e:
        logger.error(f"Failed to validate data completeness: {e}")
        return {}

def main():
    """Main execution function"""
    logger = setup_logging()
    logger.info("Starting NFL data extraction pipeline")
    
    db = None
    try:
        # Initialize database
        logger.info("Initializing database")
        db = initialize_database()
        
        # Extract teams (required first for mappings)
        teams_result = extract_and_store_teams(db, logger)
        if not teams_result['success']:
            logger.error("Cannot continue without teams data")
            return 1
        
        team_mappings = teams_result['mappings']
        
        # Extract games
        games_result = extract_and_store_games(db, team_mappings, logger)
        if not games_result['success']:
            logger.warning("Games extraction failed, but continuing with players")
        
        # Extract players (start with most recent season)
        players_result = extract_and_store_players(db, team_mappings, logger, 
                                                  seasons=[SEASONS[-1]])
        if not players_result['success']:
            logger.warning("Players extraction failed")
        
        # Validate data completeness
        validate_database_completeness(db, logger)
        
        logger.info("🎉 Data extraction pipeline completed!")
        logger.info(f"Database location: {db.db_path}")
        
        # Print next steps
        logger.info("\n=== NEXT STEPS ===")
        logger.info("1. Run statistical data extraction scripts")
        logger.info("2. Calculate fantasy points")
        logger.info("3. Build ML models for predictions")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        return 1
        
    finally:
        if db:
            db.disconnect()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)