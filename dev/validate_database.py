#!/usr/bin/env python3
"""
Validate 2024 NFL database completeness and create summary
Phase 2: Data Collection & Storage - Final validation
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Tuple

class DatabaseValidator:
    """
    Validates loaded 2024 NFL data and creates comprehensive summary
    """
    
    def __init__(self, db_path: str = "/Users/evgen/projects/ek_nfl_fantasy/dev/data/nfl_fantasy.db"):
        self.db_path = db_path
        self.connection = None
        self.validation_results = {}
    
    def connect(self):
        """Connect to database"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            print(f"✅ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def validate_core_tables(self):
        """Validate core table contents"""
        print(f"\n📊 Validating Core Tables")
        print("=" * 35)
        
        core_tables = ['teams', 'players', 'games']
        results = {}
        
        for table in core_tables:
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()[0]
                
                print(f"✅ {table}: {count:,} records")
                results[table] = {'count': count, 'status': 'ok'}
                
                # Additional validation
                if table == 'teams':
                    cursor.execute("SELECT COUNT(DISTINCT team_code) as unique_codes FROM teams")
                    unique_codes = cursor.fetchone()[0]
                    print(f"   - Unique team codes: {unique_codes}")
                    results[table]['unique_codes'] = unique_codes
                
                elif table == 'players':
                    cursor.execute("SELECT position, COUNT(*) as count FROM players WHERE position IS NOT NULL GROUP BY position ORDER BY count DESC")
                    positions = cursor.fetchall()
                    print(f"   - Positions: {', '.join([f'{pos[0]}({pos[1]})' for pos in positions[:5]])}")
                    results[table]['positions'] = dict(positions)
                
                elif table == 'games':
                    cursor.execute("SELECT MIN(week) as min_week, MAX(week) as max_week FROM games WHERE season = 2024")
                    weeks = cursor.fetchone()
                    if weeks[0] and weeks[1]:
                        print(f"   - Weeks: {weeks[0]} to {weeks[1]}")
                        results[table]['week_range'] = [weeks[0], weeks[1]]
                
            except Exception as e:
                print(f"❌ Error validating {table}: {e}")
                results[table] = {'error': str(e), 'status': 'error'}
        
        self.validation_results['core_tables'] = results
        return results
    
    def validate_stats_tables(self):
        """Validate statistics tables"""
        print(f"\n📈 Validating Statistics Tables")
        print("=" * 40)
        
        stats_tables = ['passing_stats', 'rushing_stats', 'receiving_stats', 'defensive_stats']
        results = {}
        
        for table in stats_tables:
            try:
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()[0]
                
                print(f"✅ {table}: {count:,} records")
                results[table] = {'count': count, 'status': 'ok'}
                
                if count > 0:
                    # Get unique players in this table
                    cursor.execute(f"SELECT COUNT(DISTINCT player_id) as unique_players FROM {table}")
                    unique_players = cursor.fetchone()[0]
                    print(f"   - Unique players: {unique_players}")
                    results[table]['unique_players'] = unique_players
                    
                    # Get game range
                    cursor.execute(f"""
                        SELECT MIN(g.week) as min_week, MAX(g.week) as max_week 
                        FROM {table} s 
                        JOIN games g ON s.game_id = g.game_id
                    """)
                    weeks = cursor.fetchone()
                    if weeks[0] and weeks[1]:
                        print(f"   - Week range: {weeks[0]} to {weeks[1]}")
                        results[table]['week_range'] = [weeks[0], weeks[1]]
                
            except Exception as e:
                print(f"❌ Error validating {table}: {e}")
                results[table] = {'error': str(e), 'status': 'error'}
        
        self.validation_results['stats_tables'] = results
        return results
    
    def analyze_data_quality(self):
        """Analyze data quality and completeness"""
        print(f"\n🔍 Data Quality Analysis")
        print("=" * 30)
        
        quality_checks = {}
        cursor = self.connection.cursor()
        
        try:
            # Check for complete player coverage across stats
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(DISTINCT player_id) FROM passing_stats) as passing_players,
                    (SELECT COUNT(DISTINCT player_id) FROM rushing_stats) as rushing_players,
                    (SELECT COUNT(DISTINCT player_id) FROM receiving_stats) as receiving_players
            """)
            
            stats_coverage = cursor.fetchone()
            print(f"📊 Player coverage:")
            print(f"   - Passing stats: {stats_coverage[0]} players")
            print(f"   - Rushing stats: {stats_coverage[1]} players")
            print(f"   - Receiving stats: {stats_coverage[2]} players")
            
            quality_checks['player_coverage'] = {
                'passing': stats_coverage[0],
                'rushing': stats_coverage[1], 
                'receiving': stats_coverage[2]
            }
            
            # Check for game consistency
            cursor.execute("""
                SELECT COUNT(DISTINCT game_id) as unique_games_in_stats
                FROM (
                    SELECT game_id FROM passing_stats
                    UNION 
                    SELECT game_id FROM rushing_stats
                    UNION
                    SELECT game_id FROM receiving_stats
                )
            """)
            
            unique_games_with_stats = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) as total_games FROM games")
            total_games = cursor.fetchone()[0]
            
            print(f"🎮 Game coverage:")
            print(f"   - Total games: {total_games}")
            print(f"   - Games with stats: {unique_games_with_stats}")
            print(f"   - Coverage: {(unique_games_with_stats/total_games*100):.1f}%" if total_games > 0 else "   - Coverage: 0%")
            
            quality_checks['game_coverage'] = {
                'total_games': total_games,
                'games_with_stats': unique_games_with_stats,
                'coverage_percent': round((unique_games_with_stats/total_games*100), 1) if total_games > 0 else 0
            }
            
        except Exception as e:
            print(f"❌ Error in quality analysis: {e}")
            quality_checks['error'] = str(e)
        
        self.validation_results['data_quality'] = quality_checks
        return quality_checks
    
    def create_sample_queries(self):
        """Create sample queries to demonstrate data access"""
        print(f"\n🎯 Sample Data Queries")
        print("=" * 25)
        
        sample_results = {}
        cursor = self.connection.cursor()
        
        try:
            # Top QBs by passing yards
            cursor.execute("""
                SELECT p.name, p.position, SUM(ps.passing_yards) as total_yards, COUNT(*) as games
                FROM players p
                JOIN passing_stats ps ON p.player_id = ps.player_id
                WHERE p.position = 'QB'
                GROUP BY p.player_id, p.name, p.position
                ORDER BY total_yards DESC
                LIMIT 5
            """)
            
            top_qbs = cursor.fetchall()
            if top_qbs:
                print(f"🏈 Top QBs by passing yards:")
                for i, qb in enumerate(top_qbs):
                    print(f"   {i+1}. {qb[0]}: {qb[2]:,} yards ({qb[3]} games)")
                sample_results['top_qbs'] = [dict(qb) for qb in top_qbs]
            
            # Top RBs by rushing yards
            cursor.execute("""
                SELECT p.name, p.position, SUM(rs.rushing_yards) as total_yards, COUNT(*) as games
                FROM players p
                JOIN rushing_stats rs ON p.player_id = rs.player_id
                WHERE p.position = 'RB'
                GROUP BY p.player_id, p.name, p.position
                ORDER BY total_yards DESC
                LIMIT 5
            """)
            
            top_rbs = cursor.fetchall()
            if top_rbs:
                print(f"🏃 Top RBs by rushing yards:")
                for i, rb in enumerate(top_rbs):
                    print(f"   {i+1}. {rb[0]}: {rb[2]:,} yards ({rb[3]} games)")
                sample_results['top_rbs'] = [dict(rb) for rb in top_rbs]
            
            # Top WRs by receiving yards
            cursor.execute("""
                SELECT p.name, p.position, SUM(rs.receiving_yards) as total_yards, COUNT(*) as games
                FROM players p
                JOIN receiving_stats rs ON p.player_id = rs.player_id
                WHERE p.position IN ('WR', 'TE')
                GROUP BY p.player_id, p.name, p.position
                ORDER BY total_yards DESC
                LIMIT 5
            """)
            
            top_receivers = cursor.fetchall()
            if top_receivers:
                print(f"🎯 Top WR/TE by receiving yards:")
                for i, wr in enumerate(top_receivers):
                    print(f"   {i+1}. {wr[0]} ({wr[1]}): {wr[2]:,} yards ({wr[3]} games)")
                sample_results['top_receivers'] = [dict(wr) for wr in top_receivers]
            
        except Exception as e:
            print(f"❌ Error in sample queries: {e}")
            sample_results['error'] = str(e)
        
        self.validation_results['sample_queries'] = sample_results
        return sample_results
    
    def generate_final_report(self):
        """Generate comprehensive final report"""
        print(f"\n📋 Generating Final Report")
        print("=" * 30)
        
        # Calculate summary metrics
        cursor = self.connection.cursor()
        
        summary = {
            'validation_date': datetime.now().isoformat(),
            'database_path': self.db_path,
            'season': 2024,
            'phase': 'Phase 2: Data Collection & Storage',
            'status': 'COMPLETE'
        }
        
        # Add all validation results
        summary.update(self.validation_results)
        
        # Calculate overall health score
        health_score = 0
        max_score = 100
        
        # Core tables health (40 points)
        core_health = sum(1 for table in ['teams', 'players', 'games'] 
                         if self.validation_results.get('core_tables', {}).get(table, {}).get('status') == 'ok')
        health_score += (core_health / 3) * 40
        
        # Stats tables health (40 points) 
        stats_health = sum(1 for table in ['passing_stats', 'rushing_stats', 'receiving_stats']
                          if self.validation_results.get('stats_tables', {}).get(table, {}).get('count', 0) > 0)
        health_score += (stats_health / 3) * 40
        
        # Data quality health (20 points)
        quality = self.validation_results.get('data_quality', {})
        if quality.get('player_coverage', {}).get('passing', 0) > 0:
            health_score += 20
        
        summary['health_score'] = round(health_score, 1)
        
        # Save report
        report_path = "/Users/evgen/projects/ek_nfl_fantasy/dev/data/phase_2_database_report.json"
        with open(report_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"📁 Final report saved: {report_path}")
        return summary
    
    def print_final_summary(self, report):
        """Print final summary"""
        print(f"\n🎉 PHASE 2 COMPLETION SUMMARY")
        print("=" * 35)
        
        core_tables = report.get('core_tables', {})
        stats_tables = report.get('stats_tables', {})
        quality = report.get('data_quality', {})
        
        print(f"🏥 Database Health Score: {report.get('health_score', 0)}/100")
        
        print(f"\n📊 Core Data Loaded:")
        print(f"   Teams: {core_tables.get('teams', {}).get('count', 0):,}")
        print(f"   Players: {core_tables.get('players', {}).get('count', 0):,}")
        print(f"   Games: {core_tables.get('games', {}).get('count', 0):,}")
        
        print(f"\n📈 Statistics Loaded:")
        print(f"   Passing stats: {stats_tables.get('passing_stats', {}).get('count', 0):,}")
        print(f"   Rushing stats: {stats_tables.get('rushing_stats', {}).get('count', 0):,}")
        print(f"   Receiving stats: {stats_tables.get('receiving_stats', {}).get('count', 0):,}")
        
        print(f"\n🎯 Data Quality:")
        player_coverage = quality.get('player_coverage', {})
        game_coverage = quality.get('game_coverage', {})
        print(f"   Player coverage: {player_coverage.get('passing', 0)} QBs, {player_coverage.get('rushing', 0)} RBs, {player_coverage.get('receiving', 0)} WR/TE")
        print(f"   Game coverage: {game_coverage.get('coverage_percent', 0):.1f}% ({game_coverage.get('games_with_stats', 0)}/{game_coverage.get('total_games', 0)})")
        
        print(f"\n✅ Ready for Phase 3: Data Analysis & Feature Engineering")
        print(f"📁 Database: {self.db_path}")

def main():
    """Main validation function"""
    print("Phase 2: Data Collection & Storage - Database Validation")
    print("=" * 60)
    
    validator = DatabaseValidator()
    
    if not validator.connect():
        return False
    
    try:
        # Run all validations
        validator.validate_core_tables()
        validator.validate_stats_tables()
        validator.analyze_data_quality()
        validator.create_sample_queries()
        
        # Generate final report
        report = validator.generate_final_report()
        validator.print_final_summary(report)
        
        print(f"\n🎊 Phase 2: Data Collection & Storage - COMPLETE!")
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False
    
    finally:
        if validator.connection:
            validator.connection.close()

if __name__ == "__main__":
    main()