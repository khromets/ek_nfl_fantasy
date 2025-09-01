#!/usr/bin/env python3
"""
Load individual player game logs into database
Phase 2: Data Collection & Storage - Game log data loading
"""

import pandas as pd
import sqlite3
import os
import glob
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import re

class GameLogLoader:
    """
    Loads individual player game logs into the database
    """
    
    def __init__(self, data_dir: str = "/Users/evgen/projects/ek_nfl_fantasy/dev/data"):
        self.data_dir = data_dir
        self.db_path = os.path.join(data_dir, "nfl_fantasy.db")
        self.connection = None
        self.season = 2024
    
    def connect_db(self):
        """Connect to database"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            print(f"✅ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def get_player_id_by_name(self, player_name: str, position: str = None) -> Optional[int]:
        """Get player ID from name, creating if not exists"""
        if not player_name or not self.connection:
            return None
        
        try:
            cursor = self.connection.cursor()
            
            # First try exact match
            if position:
                cursor.execute("SELECT player_id FROM players WHERE name = ? AND position = ?", 
                             (player_name, position))
            else:
                cursor.execute("SELECT player_id FROM players WHERE name = ?", (player_name,))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # If not found and we have position, create the player
            if position:
                try:
                    cursor.execute('''
                        INSERT INTO players (name, position, active, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (player_name, position, True, datetime.now(), datetime.now()))
                    
                    self.connection.commit()
                    return cursor.lastrowid
                except:
                    pass
            
            return None
            
        except Exception as e:
            print(f"Warning: Could not get/create player ID for {player_name}: {e}")
            return None
    
    def create_or_get_game_id(self, week: int, date_str: str, opponent: str = None) -> Optional[int]:
        """Create or get game ID"""
        if not week or not self.connection:
            return None
        
        try:
            cursor = self.connection.cursor()
            
            # Parse date
            try:
                if isinstance(date_str, str):
                    game_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                else:
                    game_date = date_str
            except:
                game_date = date(2024, 9, 1)  # Fallback date
            
            # Check if game exists
            cursor.execute('''
                SELECT game_id FROM games 
                WHERE season = ? AND week = ? AND game_date = ?
            ''', (self.season, week, game_date))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # Create new game (minimal data)
            cursor.execute('''
                INSERT INTO games 
                (season, week, game_date, home_team_id, away_team_id, game_type)
                VALUES (?, ?, ?, 1, 1, 'REG')
            ''', (self.season, week, game_date))
            
            self.connection.commit()
            return cursor.lastrowid
            
        except Exception as e:
            print(f"Warning: Could not create/get game ID for week {week}: {e}")
            return None
    
    def load_player_game_log(self, filepath: str) -> bool:
        """Load individual player game log file"""
        if not os.path.exists(filepath):
            return False
        
        filename = os.path.basename(filepath)
        
        # Extract player info from filename
        # Format: 2024_QB_Joe_Burrow_game_log.csv
        parts = filename.replace('_game_log.csv', '').split('_')
        if len(parts) < 4:
            print(f"❌ Invalid filename format: {filename}")
            return False
        
        position = parts[1]
        player_name = ' '.join(parts[2:])  # Join all remaining parts as name
        
        # Handle special characters in names
        player_name = player_name.replace('JaMarr', 'Ja\'Marr')
        player_name = player_name.replace('Amon-Ra St Brown', 'Amon-Ra St. Brown')
        
        print(f"🏈 Loading {player_name} ({position}) game log...")
        
        try:
            df = pd.read_csv(filepath)
            
            if df.empty:
                print(f"❌ Empty file: {filename}")
                return False
            
            # Get player ID
            player_id = self.get_player_id_by_name(player_name, position)
            if not player_id:
                print(f"❌ Could not get player ID for {player_name}")
                return False
            
            games_loaded = 0
            cursor = self.connection.cursor()
            
            for _, row in df.iterrows():
                # Extract game info
                week = row.get('Week')
                game_date = row.get('Date')
                opponent = row.get('Opp', '')
                
                if pd.isna(week) or pd.isna(game_date):
                    continue
                
                try:
                    week = int(float(week))
                except:
                    continue
                
                # Get game ID
                game_id = self.create_or_get_game_id(week, str(game_date), opponent)
                if not game_id:
                    continue
                
                # Load stats based on position
                if position == 'QB':
                    self._load_qb_stats(cursor, player_id, game_id, row)
                elif position == 'RB':
                    self._load_rb_stats(cursor, player_id, game_id, row)
                elif position in ['WR', 'TE']:
                    self._load_wr_te_stats(cursor, player_id, game_id, row)
                
                games_loaded += 1
            
            self.connection.commit()
            print(f"✅ Loaded {games_loaded} games for {player_name}")
            return games_loaded > 0
            
        except Exception as e:
            print(f"❌ Error loading {filename}: {e}")
            return False
    
    def _load_qb_stats(self, cursor, player_id: int, game_id: int, row):
        """Load QB passing stats"""
        try:
            # Extract QB stats
            completions = self._safe_int(row.get('Cmp'))
            attempts = self._safe_int(row.get('Att'))
            passing_yards = self._safe_int(row.get('Yds'))
            passing_tds = self._safe_int(row.get('TD'))
            interceptions = self._safe_int(row.get('Int'))
            
            cursor.execute('''
                INSERT OR REPLACE INTO passing_stats 
                (player_id, game_id, attempts, completions, passing_yards, 
                 passing_touchdowns, interceptions, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (player_id, game_id, attempts, completions, passing_yards, 
                  passing_tds, interceptions, datetime.now()))
            
        except Exception as e:
            print(f"Warning: Could not load QB stats: {e}")
    
    def _load_rb_stats(self, cursor, player_id: int, game_id: int, row):
        """Load RB rushing stats"""
        try:
            # Extract RB stats
            rushing_attempts = self._safe_int(row.get('Att'))
            rushing_yards = self._safe_int(row.get('Yds'))
            rushing_tds = self._safe_int(row.get('TD'))
            fumbles = self._safe_int(row.get('Fmb'))
            
            # Also get receiving stats if available
            receptions = self._safe_int(row.get('Rec'))
            receiving_yards = self._safe_int(row.get('Rec Yds'))
            receiving_tds = self._safe_int(row.get('Rec TD'))
            
            # Insert rushing stats
            cursor.execute('''
                INSERT OR REPLACE INTO rushing_stats 
                (player_id, game_id, attempts, rushing_yards, rushing_touchdowns, fumbles, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (player_id, game_id, rushing_attempts, rushing_yards, rushing_tds, fumbles, datetime.now()))
            
            # Insert receiving stats if available
            if receptions is not None or receiving_yards is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO receiving_stats 
                    (player_id, game_id, receptions, receiving_yards, receiving_touchdowns, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (player_id, game_id, receptions, receiving_yards, receiving_tds, datetime.now()))
            
        except Exception as e:
            print(f"Warning: Could not load RB stats: {e}")
    
    def _load_wr_te_stats(self, cursor, player_id: int, game_id: int, row):
        """Load WR/TE receiving stats"""
        try:
            # Extract receiving stats
            targets = self._safe_int(row.get('Tgt'))
            receptions = self._safe_int(row.get('Rec'))
            receiving_yards = self._safe_int(row.get('Yds'))
            receiving_tds = self._safe_int(row.get('TD'))
            
            cursor.execute('''
                INSERT OR REPLACE INTO receiving_stats 
                (player_id, game_id, targets, receptions, receiving_yards, receiving_touchdowns, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (player_id, game_id, targets, receptions, receiving_yards, receiving_tds, datetime.now()))
            
        except Exception as e:
            print(f"Warning: Could not load WR/TE stats: {e}")
    
    def _safe_int(self, value) -> Optional[int]:
        """Safely convert value to int"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '')  # Remove commas
            return int(float(value))
        except:
            return None
    
    def load_all_game_logs(self) -> Dict[str, int]:
        """Load all game log files"""
        print(f"\n🎮 Loading All Player Game Logs")
        print("=" * 40)
        
        # Find all game log files
        pattern = os.path.join(self.data_dir, "2024_*_game_log.csv")
        game_log_files = glob.glob(pattern)
        
        if not game_log_files:
            print(f"❌ No game log files found")
            return {}
        
        print(f"📁 Found {len(game_log_files)} game log files")
        
        results = {'loaded': 0, 'failed': 0}
        position_counts = {'QB': 0, 'RB': 0, 'WR': 0, 'TE': 0}
        
        for filepath in game_log_files:
            filename = os.path.basename(filepath)
            
            # Extract position
            position = None
            for pos in ['QB', 'RB', 'WR', 'TE']:
                if f"2024_{pos}_" in filename:
                    position = pos
                    break
            
            if self.load_player_game_log(filepath):
                results['loaded'] += 1
                if position:
                    position_counts[position] += 1
            else:
                results['failed'] += 1
                print(f"❌ Failed to load: {filename}")
        
        print(f"\n📊 Game Log Loading Summary:")
        print(f"✅ Successfully loaded: {results['loaded']}")
        print(f"❌ Failed to load: {results['failed']}")
        
        print(f"\n🎯 By Position:")
        for pos, count in position_counts.items():
            if count > 0:
                print(f"   {pos}: {count} players")
        
        return results

def main():
    """Main game log loading function"""
    print("Phase 2: Loading Individual Player Game Logs")
    print("=" * 50)
    
    loader = GameLogLoader()
    
    if not loader.connect_db():
        return False
    
    try:
        results = loader.load_all_game_logs()
        
        # Database summary after loading
        cursor = loader.connection.cursor()
        
        # Count stats
        cursor.execute("SELECT COUNT(*) FROM passing_stats")
        passing_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM rushing_stats")  
        rushing_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM receiving_stats")
        receiving_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM games")
        games_count = cursor.fetchone()[0]
        
        print(f"\n🎉 GAME LOG LOADING COMPLETE")
        print("=" * 35)
        print(f"📊 Database Statistics:")
        print(f"   Games: {games_count}")
        print(f"   Passing stats: {passing_count}")
        print(f"   Rushing stats: {rushing_count}")
        print(f"   Receiving stats: {receiving_count}")
        
        return results['loaded'] > 0
        
    except Exception as e:
        print(f"❌ Loading failed: {e}")
        return False
    
    finally:
        if loader.connection:
            loader.connection.close()

if __name__ == "__main__":
    main()