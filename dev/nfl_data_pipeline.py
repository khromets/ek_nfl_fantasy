#!/usr/bin/env python3
"""
Complete NFL Data Collection Pipeline
Phase 1.2 - Step 3: Comprehensive Data Extraction Strategy
"""

import pandas as pd
import time
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Import our extractors
from test_game_logs import ManualGameLogExtractor

class NFLDataPipeline:
    """
    Comprehensive NFL data extraction pipeline combining season stats and game logs
    """
    
    def __init__(self, data_dir: str = "/Users/evgen/projects/ek_nfl_fantasy/dev/data"):
        self.data_dir = data_dir
        self.game_log_extractor = ManualGameLogExtractor()
        
        # Ensure data directory exists
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # Known top players by position (manually curated for now)
        self.top_players = {
            'QB': [
                ('Joe Burrow', '/players/B/BurrJo01.htm'),
                ('Josh Allen', '/players/A/AlleJo02.htm'),
                ('Patrick Mahomes', '/players/M/MahoPa00.htm'),
                ('Lamar Jackson', '/players/J/JackLa00.htm'),
                ('Jalen Hurts', '/players/H/HurtJa00.htm'),
                ('Dak Prescott', '/players/P/PresDa01.htm'),
                ('Justin Herbert', '/players/H/HerbJu00.htm'),
                ('Tua Tagovailoa', '/players/T/TagoTu00.htm'),
                ('Brock Purdy', '/players/P/PurdBr00.htm'),
                ('Geno Smith', '/players/S/SmitGe00.htm')
            ],
            'RB': [
                ('Saquon Barkley', '/players/B/BarkSa00.htm'),
                ('Derrick Henry', '/players/H/HenrDe00.htm'),
                ('Christian McCaffrey', '/players/M/McCaCh01.htm'),
                ('Josh Jacobs', '/players/J/JacoJo01.htm'),
                ('Bijan Robinson', '/players/R/RobiBi00.htm'),
                ('Joe Mixon', '/players/M/MixoJo00.htm'),
                ('Jonathan Taylor', '/players/T/TaylJo02.htm'),
                ('Alvin Kamara', '/players/K/KamaAl00.htm'),
                ('Kenneth Walker III', '/players/W/WalkKe02.htm'),
                ('Tony Pollard', '/players/P/PollTo01.htm')
            ],
            'WR': [
                ('Justin Jefferson', '/players/J/JeffJu00.htm'),
                ('CeeDee Lamb', '/players/L/LambCe00.htm'),
                ('Tyreek Hill', '/players/H/HillTy00.htm'),
                ('Ja\'Marr Chase', '/players/C/ChasJa00.htm'),
                ('Amon-Ra St. Brown', '/players/S/StxxAm00.htm'),
                ('A.J. Brown', '/players/B/BrowAJ00.htm'),
                ('Mike Evans', '/players/E/EvanMi00.htm'),
                ('Davante Adams', '/players/A/AdamDa01.htm'),
                ('DeVonta Smith', '/players/S/SmitDe05.htm'),
                ('DK Metcalf', '/players/M/MetcDK00.htm')
            ],
            'TE': [
                ('Travis Kelce', '/players/K/KelcTr00.htm'),
                ('George Kittle', '/players/K/KittGe00.htm'),
                ('Mark Andrews', '/players/A/AndrMa00.htm'),
                ('T.J. Hockenson', '/players/H/HockTJ00.htm'),
                ('Sam LaPorta', '/players/L/LaPoBa01.htm'),
                ('Kyle Pitts', '/players/P/PittKy00.htm'),
                ('Dallas Goedert', '/players/G/GoedDa00.htm'),
                ('Evan Engram', '/players/E/EngrEv00.htm'),
                ('David Njoku', '/players/N/NjokuDa00.htm'),
                ('Jake Ferguson', '/players/F/FergJa01.htm')
            ]
        }
    
    def extract_position_game_logs(self, position: str, year: int = 2024, 
                                 max_players: int = 10) -> Dict[str, pd.DataFrame]:
        """
        Extract game logs for top players at a position
        
        Args:
            position: Position code ('QB', 'RB', 'WR', 'TE')
            year: Season year
            max_players: Maximum number of players to extract
            
        Returns:
            Dictionary mapping player names to their game log DataFrames
        """
        if position not in self.top_players:
            print(f"❌ Unsupported position: {position}")
            return {}
        
        players_to_extract = self.top_players[position][:max_players]
        
        print(f"\n🎯 Extracting {position} Game Logs ({year})")
        print(f"📋 Players: {len(players_to_extract)}")
        print("=" * 50)
        
        game_logs = {}
        
        for i, (player_name, player_url) in enumerate(players_to_extract):
            print(f"🏈 {i+1}/{len(players_to_extract)}: {player_name}")
            
            try:
                game_log_df = self.game_log_extractor.extract_game_log(player_url, year)
                
                if not game_log_df.empty:
                    game_logs[player_name] = game_log_df
                    
                    # Save individual player file
                    filename = f"{year}_{position}_{player_name.replace(' ', '_').replace('.', '').replace('\'', '')}_game_log.csv"
                    filepath = os.path.join(self.data_dir, filename)
                    game_log_df.to_csv(filepath, index=False)
                    
                    print(f"✅ Extracted {len(game_log_df)} games → {filename}")
                    
                    # Show key stats sample
                    if len(game_log_df) > 0:
                        sample_stats = []
                        sample_game = game_log_df.iloc[0]
                        
                        # Position-specific key stats
                        key_cols = {
                            'QB': ['Cmp', 'Att', 'Yds', 'TD', 'Int'],
                            'RB': ['Att', 'Yds', 'TD', 'Rec', 'Rec Yds'],
                            'WR': ['Tgt', 'Rec', 'Yds', 'TD', 'Lng'],
                            'TE': ['Tgt', 'Rec', 'Yds', 'TD']
                        }
                        
                        for col in key_cols.get(position, []):
                            if col in game_log_df.columns:
                                value = sample_game.get(col, 'N/A')
                                sample_stats.append(f"{col}: {value}")
                        
                        if sample_stats:
                            print(f"📊 Sample stats: {' | '.join(sample_stats[:4])}")
                
                else:
                    print(f"❌ No data extracted for {player_name}")
                
            except Exception as e:
                print(f"❌ Error extracting {player_name}: {e}")
            
            # Progress delay
            if i < len(players_to_extract) - 1:
                time.sleep(2)  # Be respectful to the server
        
        print(f"\n✅ {position} extraction complete: {len(game_logs)}/{len(players_to_extract)} successful")
        return game_logs
    
    def extract_all_positions(self, year: int = 2024, 
                            players_per_position: int = 10) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Extract game logs for all fantasy-relevant positions
        
        Args:
            year: Season year
            players_per_position: Number of top players per position
            
        Returns:
            Nested dictionary: position -> player_name -> DataFrame
        """
        print(f"🏈 NFL {year} Complete Game Log Extraction")
        print(f"📊 Target: {players_per_position} players × {len(self.top_players)} positions")
        print("=" * 60)
        
        all_results = {}
        extraction_summary = {
            'extraction_date': datetime.now().isoformat(),
            'season': year,
            'players_per_position': players_per_position,
            'positions': {}
        }
        
        for position in ['QB', 'RB', 'WR', 'TE']:
            try:
                position_results = self.extract_position_game_logs(
                    position, year, players_per_position
                )
                
                all_results[position] = position_results
                
                # Summary stats
                total_games = sum(len(df) for df in position_results.values())
                successful_players = len(position_results)
                
                extraction_summary['positions'][position] = {
                    'players_extracted': successful_players,
                    'total_games': total_games,
                    'success_rate': f"{successful_players}/{players_per_position}",
                    'files_created': [
                        f"{year}_{position}_{name.replace(' ', '_').replace('.', '').replace('\'', '')}_game_log.csv"
                        for name in position_results.keys()
                    ]
                }
                
                print(f"🎯 {position}: {successful_players} players, {total_games} games")
                
                # Brief pause between positions
                time.sleep(3)
                
            except Exception as e:
                print(f"❌ Error extracting {position}: {e}")
                extraction_summary['positions'][position] = {
                    'error': str(e),
                    'players_extracted': 0,
                    'total_games': 0
                }
        
        # Save comprehensive summary
        summary_path = os.path.join(self.data_dir, f"{year}_complete_game_logs_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(extraction_summary, f, indent=2)
        
        # Final report
        total_players = sum(len(pos_results) for pos_results in all_results.values())
        total_games = sum(
            sum(len(df) for df in pos_results.values()) 
            for pos_results in all_results.values()
        )
        
        print(f"\n🎉 COMPLETE EXTRACTION SUMMARY")
        print("=" * 40)
        print(f"✅ Total players extracted: {total_players}")
        print(f"📊 Total games extracted: {total_games:,}")
        print(f"📁 Summary saved: {summary_path}")
        
        return all_results
    
    def create_combined_dataset(self, results: Dict[str, Dict[str, pd.DataFrame]], 
                              year: int = 2024) -> pd.DataFrame:
        """
        Combine all game logs into a single dataset for analysis
        
        Args:
            results: Results from extract_all_positions
            year: Season year
            
        Returns:
            Combined DataFrame with all player game logs
        """
        print(f"\n📊 Creating Combined Dataset")
        print("=" * 30)
        
        all_game_logs = []
        
        for position, player_results in results.items():
            for player_name, df in player_results.items():
                if not df.empty:
                    # Add player and position info
                    df_copy = df.copy()
                    df_copy['player_name'] = player_name
                    df_copy['position'] = position
                    
                    all_game_logs.append(df_copy)
        
        if not all_game_logs:
            print("❌ No game logs to combine")
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(all_game_logs, ignore_index=True, sort=False)
        
        # Save combined dataset
        combined_path = os.path.join(self.data_dir, f"{year}_combined_game_logs.csv")
        combined_df.to_csv(combined_path, index=False)
        
        print(f"✅ Combined dataset created: {len(combined_df):,} total games")
        print(f"📁 Saved: {combined_path}")
        
        # Show breakdown by position
        position_counts = combined_df['position'].value_counts()
        for pos, count in position_counts.items():
            print(f"   {pos}: {count:,} games")
        
        return combined_df

def main():
    """Main pipeline execution"""
    print("Phase 1.2 Step 3: Complete NFL Data Pipeline")
    print("=" * 55)
    
    # Initialize pipeline
    pipeline = NFLDataPipeline()
    
    # Extract game logs for all positions (2024 season)
    results = pipeline.extract_all_positions(
        year=2024,
        players_per_position=8  # Top 8 players per position
    )
    
    # Create combined dataset
    if results:
        combined_df = pipeline.create_combined_dataset(results, 2024)
        
        if not combined_df.empty:
            print(f"\n🎯 Pipeline Complete!")
            print(f"📊 Ready for fantasy analysis with {len(combined_df):,} game records")
            print(f"📁 All data saved to: {pipeline.data_dir}")
        else:
            print(f"❌ No combined dataset created")
    else:
        print(f"❌ No results from extraction")

if __name__ == "__main__":
    main()