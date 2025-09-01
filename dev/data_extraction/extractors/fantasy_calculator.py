"""
Fantasy points calculation engine
"""
import logging
from typing import Dict, List, Any, Optional
from ..core.config import FANTASY_SCORING
from ..core.database import DatabaseManager

class FantasyPointsCalculator:
    """
    Calculate fantasy points based on player statistics and scoring rules
    """
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        self.scoring_rules = FANTASY_SCORING.copy()
    
    def calculate_passing_points(self, stats: Dict[str, Any]) -> float:
        """
        Calculate fantasy points from passing statistics
        
        Args:
            stats: Dictionary with passing stats
            
        Returns:
            Total passing fantasy points
        """
        points = 0.0
        
        # Passing yards
        yards = stats.get('passing_yards', 0)
        points += yards * self.scoring_rules['passing_yards']
        
        # Passing touchdowns
        tds = stats.get('passing_tds', 0)
        points += tds * self.scoring_rules['passing_tds']
        
        # Interceptions thrown (negative points)
        ints = stats.get('interceptions', 0)
        points += ints * self.scoring_rules['interceptions_thrown']
        
        # Two-point conversions
        two_pts = stats.get('two_point_conversions', 0)
        points += two_pts * self.scoring_rules['two_point_pass']
        
        return round(points, 2)
    
    def calculate_rushing_points(self, stats: Dict[str, Any]) -> float:
        """
        Calculate fantasy points from rushing statistics
        
        Args:
            stats: Dictionary with rushing stats
            
        Returns:
            Total rushing fantasy points
        """
        points = 0.0
        
        # Rushing yards
        yards = stats.get('rushing_yards', 0)
        points += yards * self.scoring_rules['rushing_yards']
        
        # Rushing touchdowns
        tds = stats.get('rushing_tds', 0)
        points += tds * self.scoring_rules['rushing_tds']
        
        # Two-point conversions
        two_pts = stats.get('two_point_conversions', 0)
        points += two_pts * self.scoring_rules['two_point_rush']
        
        # Fumbles lost (negative points)
        fumbles_lost = stats.get('fumbles_lost', 0)
        points += fumbles_lost * self.scoring_rules['fumbles_lost']
        
        return round(points, 2)
    
    def calculate_receiving_points(self, stats: Dict[str, Any]) -> float:
        """
        Calculate fantasy points from receiving statistics
        
        Args:
            stats: Dictionary with receiving stats
            
        Returns:
            Total receiving fantasy points
        """
        points = 0.0
        
        # Receiving yards
        yards = stats.get('receiving_yards', 0)
        points += yards * self.scoring_rules['receiving_yards']
        
        # Receptions (PPR)
        receptions = stats.get('receptions', 0)
        points += receptions * self.scoring_rules['receptions']
        
        # Receiving touchdowns
        tds = stats.get('receiving_tds', 0)
        points += tds * self.scoring_rules['receiving_tds']
        
        # Two-point conversions
        two_pts = stats.get('two_point_conversions', 0)
        points += two_pts * self.scoring_rules['two_point_reception']
        
        # Fumbles lost (negative points)
        fumbles_lost = stats.get('fumbles_lost', 0)
        points += fumbles_lost * self.scoring_rules['fumbles_lost']
        
        return round(points, 2)
    
    def calculate_defensive_points(self, stats: Dict[str, Any]) -> float:
        """
        Calculate fantasy points from individual defensive statistics
        
        Args:
            stats: Dictionary with defensive stats
            
        Returns:
            Total defensive fantasy points
        """
        points = 0.0
        
        # Tackles
        solo_tackles = stats.get('tackles_solo', 0)
        assisted_tackles = stats.get('tackles_assisted', 0)
        points += solo_tackles * self.scoring_rules['tackles_solo']
        points += assisted_tackles * self.scoring_rules['tackles_assisted']
        
        # Sacks
        sacks = stats.get('sacks', 0)
        points += sacks * self.scoring_rules['sacks']
        
        # Interceptions
        interceptions = stats.get('interceptions', 0)
        points += interceptions * self.scoring_rules['interceptions']
        
        # Fumbles forced/recovered
        fumbles_forced = stats.get('fumbles_forced', 0)
        fumbles_recovered = stats.get('fumbles_recovered', 0)
        points += fumbles_forced * self.scoring_rules['fumbles_forced']
        points += fumbles_recovered * self.scoring_rules['fumbles_recovered']
        
        # Pass defended
        passes_defended = stats.get('passes_defended', 0)
        points += passes_defended * self.scoring_rules['passes_defended']
        
        # Safeties
        safeties = stats.get('safeties', 0)
        points += safeties * self.scoring_rules['safeties']
        
        # Defensive touchdowns
        def_tds = stats.get('defensive_tds', 0)
        points += def_tds * self.scoring_rules['defensive_tds']
        
        # Blocked kicks
        blocked_kicks = stats.get('blocked_kicks', 0)
        points += blocked_kicks * self.scoring_rules['blocked_kicks']
        
        return round(points, 2)
    
    def calculate_special_teams_points(self, stats: Dict[str, Any]) -> float:
        """
        Calculate fantasy points from special teams statistics
        
        Args:
            stats: Dictionary with special teams stats
            
        Returns:
            Total special teams fantasy points
        """
        points = 0.0
        
        # Return touchdowns
        kick_return_tds = stats.get('kick_return_tds', 0)
        punt_return_tds = stats.get('punt_return_tds', 0)
        points += kick_return_tds * self.scoring_rules['kick_return_tds']
        points += punt_return_tds * self.scoring_rules['punt_return_tds']
        
        return round(points, 2)
    
    def calculate_player_game_points(self, player_id: int, game_id: int) -> Optional[Dict[str, float]]:
        """
        Calculate total fantasy points for a player in a specific game
        
        Args:
            player_id: Player ID
            game_id: Game ID
            
        Returns:
            Dictionary with points breakdown or None if no stats found
        """
        if not self.db:
            self.logger.error("Database manager not provided")
            return None
        
        try:
            # Get player position for context
            player_info = self.db.query(
                "SELECT position FROM players WHERE player_id = ?",
                (player_id,)
            )
            
            if not player_info:
                self.logger.warning(f"Player {player_id} not found")
                return None
            
            position = player_info[0]['position']
            
            points_breakdown = {
                'passing_points': 0.0,
                'rushing_points': 0.0,
                'receiving_points': 0.0,
                'defensive_points': 0.0,
                'special_teams_points': 0.0,
                'total_points': 0.0
            }
            
            # Get passing stats
            passing_stats = self.db.query(
                """SELECT * FROM passing_stats 
                   WHERE player_id = ? AND game_id = ?""",
                (player_id, game_id)
            )
            
            if passing_stats:
                stats_dict = dict(passing_stats[0])
                points_breakdown['passing_points'] = self.calculate_passing_points(stats_dict)
            
            # Get rushing stats
            rushing_stats = self.db.query(
                """SELECT * FROM rushing_stats 
                   WHERE player_id = ? AND game_id = ?""",
                (player_id, game_id)
            )
            
            if rushing_stats:
                stats_dict = dict(rushing_stats[0])
                points_breakdown['rushing_points'] = self.calculate_rushing_points(stats_dict)
            
            # Get receiving stats
            receiving_stats = self.db.query(
                """SELECT * FROM receiving_stats 
                   WHERE player_id = ? AND game_id = ?""",
                (player_id, game_id)
            )
            
            if receiving_stats:
                stats_dict = dict(receiving_stats[0])
                points_breakdown['receiving_points'] = self.calculate_receiving_points(stats_dict)
            
            # Get defensive stats (if applicable)
            if position in ['DT', 'DE', 'LB', 'CB', 'S', 'DL', 'DB']:
                defensive_stats = self.db.query(
                    """SELECT * FROM defensive_stats 
                       WHERE player_id = ? AND game_id = ?""",
                    (player_id, game_id)
                )
                
                if defensive_stats:
                    stats_dict = dict(defensive_stats[0])
                    points_breakdown['defensive_points'] = self.calculate_defensive_points(stats_dict)
            
            # Get special teams stats
            return_stats = self.db.query(
                """SELECT * FROM return_stats 
                   WHERE player_id = ? AND game_id = ?""",
                (player_id, game_id)
            )
            
            if return_stats:
                stats_dict = dict(return_stats[0])
                points_breakdown['special_teams_points'] = self.calculate_special_teams_points(stats_dict)
            
            # Calculate total
            points_breakdown['total_points'] = sum([
                points_breakdown['passing_points'],
                points_breakdown['rushing_points'],
                points_breakdown['receiving_points'],
                points_breakdown['defensive_points'],
                points_breakdown['special_teams_points']
            ])
            
            return points_breakdown
            
        except Exception as e:
            self.logger.error(f"Failed to calculate points for player {player_id}, game {game_id}: {e}")
            return None
    
    def bulk_calculate_fantasy_points(self, season: int = None) -> int:
        """
        Calculate fantasy points for all players/games in database
        
        Args:
            season: Optional season to limit calculation
            
        Returns:
            Number of fantasy point records created
        """
        if not self.db:
            self.logger.error("Database manager not provided")
            return 0
        
        try:
            # Get all player-game combinations that need fantasy points calculated
            where_clause = "WHERE g.season = ?" if season else ""
            params = (season,) if season else ()
            
            query = f"""
                SELECT DISTINCT p.player_id, p.position, g.game_id, g.season, g.week
                FROM players p
                CROSS JOIN games g
                LEFT JOIN fantasy_points fp ON p.player_id = fp.player_id AND g.game_id = fp.game_id
                {where_clause}
                AND fp.fantasy_id IS NULL
                ORDER BY g.season, g.week, p.player_id
            """
            
            player_games = self.db.query(query, params)
            
            if not player_games:
                self.logger.info("No player-game combinations need fantasy points calculated")
                return 0
            
            self.logger.info(f"Calculating fantasy points for {len(player_games)} player-game combinations")
            
            fantasy_records = []
            processed = 0
            
            for pg in player_games:
                points = self.calculate_player_game_points(pg['player_id'], pg['game_id'])
                
                if points and points['total_points'] > 0:  # Only store if player had some stats
                    fantasy_record = {
                        'player_id': pg['player_id'],
                        'game_id': pg['game_id'],
                        'season': pg['season'],
                        'week': pg['week'],
                        'position': pg['position'],
                        'passing_points': points['passing_points'],
                        'rushing_points': points['rushing_points'],
                        'receiving_points': points['receiving_points'],
                        'defensive_points': points['defensive_points'],
                        'special_teams_points': points['special_teams_points'],
                        'total_points': points['total_points']
                    }
                    fantasy_records.append(fantasy_record)
                
                processed += 1
                if processed % 100 == 0:
                    self.logger.info(f"Processed {processed}/{len(player_games)} player-games")
            
            # Bulk insert fantasy points
            if fantasy_records:
                inserted = self.db.insert_bulk_data('fantasy_points', fantasy_records)
                self.logger.info(f"Successfully inserted {inserted} fantasy point records")
                return inserted
            else:
                self.logger.info("No fantasy point records to insert")
                return 0
                
        except Exception as e:
            self.logger.error(f"Failed to bulk calculate fantasy points: {e}")
            return 0
    
    def get_top_performers(self, position: str = None, season: int = None, 
                          limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get top fantasy performers
        
        Args:
            position: Filter by position
            season: Filter by season
            limit: Number of results to return
            
        Returns:
            List of top performers with stats
        """
        if not self.db:
            return []
        
        where_conditions = []
        params = []
        
        if position:
            where_conditions.append("fp.position = ?")
            params.append(position)
        
        if season:
            where_conditions.append("fp.season = ?")
            params.append(season)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT p.name, fp.position, fp.season,
                   COUNT(*) as games_played,
                   SUM(fp.total_points) as total_fantasy_points,
                   AVG(fp.total_points) as avg_fantasy_points,
                   MAX(fp.total_points) as best_game
            FROM fantasy_points fp
            JOIN players p ON fp.player_id = p.player_id
            {where_clause}
            GROUP BY fp.player_id, fp.season
            HAVING games_played >= 4
            ORDER BY total_fantasy_points DESC
            LIMIT ?
        """
        
        params.append(limit)
        
        try:
            results = self.db.query(query, tuple(params))
            return [dict(row) for row in results]
        except Exception as e:
            self.logger.error(f"Failed to get top performers: {e}")
            return []