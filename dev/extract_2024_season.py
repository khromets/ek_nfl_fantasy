#!/usr/bin/env python3
"""
Extract comprehensive 2024 NFL season statistics
Phase 1.2 - Step 3: Data Extraction Strategy - Phase A
"""

import pandas as pd
import time
import json
from datetime import datetime
import os

# Import our scraper
from pfr_scraper import ProFootballReferenceScraper, PositionSpecificExtractor

def save_dataframe(df: pd.DataFrame, filename: str, data_dir: str = "/Users/evgen/projects/ek_nfl_fantasy/dev/data"):
    """Save DataFrame to both CSV and JSON formats"""
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    csv_path = os.path.join(data_dir, f"{filename}.csv")
    json_path = os.path.join(data_dir, f"{filename}.json")
    
    # Save CSV
    df.to_csv(csv_path, index=False)
    print(f"📁 Saved CSV: {csv_path}")
    
    # Save JSON (for metadata and easy inspection)
    df_dict = df.to_dict('records')
    with open(json_path, 'w') as f:
        json.dump({
            'metadata': {
                'extraction_date': datetime.now().isoformat(),
                'record_count': len(df),
                'columns': list(df.columns)
            },
            'data': df_dict[:5]  # First 5 records as sample
        }, f, indent=2, default=str)
    print(f"📁 Saved JSON sample: {json_path}")
    
    return csv_path, json_path

def extract_all_positions_2024():
    """Extract season statistics for all fantasy-relevant positions in 2024"""
    
    print("🏈 NFL 2024 Season Statistics Extraction")
    print("=" * 50)
    
    # Initialize scraper
    scraper = ProFootballReferenceScraper()
    extractor = PositionSpecificExtractor(scraper)
    
    results = {}
    extraction_summary = {
        'extraction_date': datetime.now().isoformat(),
        'season': 2024,
        'positions': {}
    }
    
    # Position mapping for extraction
    positions = {
        'quarterbacks': ('QB', 'extract_qb_stats'),
        'running_backs': ('RB', 'extract_rb_stats'), 
        'receivers': ('WR/TE', 'extract_wr_te_stats'),
        'defense': ('DEF', 'extract_def_stats')
    }
    
    for pos_name, (pos_label, method_name) in positions.items():
        print(f"\n🎯 Extracting {pos_label} statistics...")
        
        try:
            # Get the extraction method
            extraction_method = getattr(extractor, method_name)
            df = extraction_method(2024)
            
            if df is not None and not df.empty:
                print(f"✅ Extracted {len(df)} {pos_label} records")
                print(f"📊 Columns: {len(df.columns)} ({list(df.columns[:5])}...)")
                
                # Show sample data
                if len(df) > 0:
                    sample_player = df.iloc[0]
                    player_name = sample_player.get('Player', 'Unknown')
                    key_stat = None
                    
                    # Find a key stat to display
                    if 'Yds' in df.columns:
                        key_stat = f"{sample_player.get('Yds', 'N/A')} yards"
                    elif 'TD' in df.columns:
                        key_stat = f"{sample_player.get('TD', 'N/A')} TDs"
                    
                    if key_stat:
                        print(f"📈 Sample: {player_name} - {key_stat}")
                
                # Save data
                filename = f"2024_{pos_name}_stats"
                csv_path, json_path = save_dataframe(df, filename)
                
                # Check for player URLs
                player_urls = scraper.extract_player_urls_from_stats(df)
                print(f"🔗 Found {len(player_urls)} player URLs for game logs")
                
                results[pos_name] = {
                    'dataframe': df,
                    'csv_path': csv_path,
                    'json_path': json_path,
                    'player_urls': player_urls,
                    'record_count': len(df)
                }
                
                extraction_summary['positions'][pos_name] = {
                    'success': True,
                    'record_count': len(df),
                    'player_urls': len(player_urls),
                    'file_paths': [csv_path, json_path]
                }
                
            else:
                print(f"❌ No data extracted for {pos_label}")
                extraction_summary['positions'][pos_name] = {
                    'success': False,
                    'error': 'No data returned'
                }
            
            # Respectful delay between extractions
            print(f"⏳ Waiting 3 seconds before next extraction...")
            time.sleep(3)
            
        except Exception as e:
            print(f"❌ Error extracting {pos_label}: {e}")
            extraction_summary['positions'][pos_name] = {
                'success': False,
                'error': str(e)
            }
    
    # Save extraction summary
    summary_path = "/Users/evgen/projects/ek_nfl_fantasy/dev/data/2024_extraction_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(extraction_summary, f, indent=2)
    
    print(f"\n📋 Extraction Summary:")
    print(f"✅ Successfully extracted: {sum(1 for pos in extraction_summary['positions'].values() if pos['success'])}/{len(positions)}")
    print(f"📁 Summary saved: {summary_path}")
    
    return results, extraction_summary

def test_player_urls():
    """Test that player URLs are now working"""
    print(f"\n🔍 Testing Player URL Extraction Fix")
    print("=" * 40)
    
    scraper = ProFootballReferenceScraper()
    
    # Get QB stats
    qb_stats = scraper.get_season_stats(2024, 'passing')
    if qb_stats is not None:
        player_urls = scraper.extract_player_urls_from_stats(qb_stats)
        print(f"✅ Extracted {len(player_urls)} player URLs from QB stats")
        
        if player_urls:
            print(f"🔗 Sample URLs:")
            for i, url in enumerate(player_urls[:5]):
                print(f"  {i+1}. {url}")
            return True
        else:
            print(f"❌ No player URLs found")
            return False
    else:
        print(f"❌ Could not extract QB stats")
        return False

def main():
    """Main extraction pipeline"""
    print("Phase 1.2 Step 3: NFL 2024 Season Data Extraction")
    print("=" * 60)
    
    # Test player URL fix first
    if not test_player_urls():
        print("❌ Player URL extraction still not working, proceeding anyway...")
    
    # Extract all position statistics
    results, summary = extract_all_positions_2024()
    
    # Summary report
    print(f"\n🎉 EXTRACTION COMPLETE")
    print("=" * 30)
    
    successful_extractions = [pos for pos, data in summary['positions'].items() if data['success']]
    total_records = sum(data['record_count'] for data in summary['positions'].values() 
                       if data['success'])
    total_urls = sum(data.get('player_urls', 0) for data in summary['positions'].values() 
                    if data['success'])
    
    print(f"✅ Positions extracted: {len(successful_extractions)}")
    print(f"📊 Total player records: {total_records:,}")
    print(f"🔗 Total player URLs: {total_urls:,}")
    print(f"📁 Data saved to: /Users/evgen/projects/ek_nfl_fantasy/dev/data/")
    
    if successful_extractions:
        print(f"\n🎯 Ready for Phase B: Game Log Extraction")
        print(f"   Available positions: {', '.join(successful_extractions)}")
    
    return results, summary

if __name__ == "__main__":
    main()