#!/usr/bin/env python3
"""
Simple test of pro-football-reference-web-scraper
"""
import time

def test_simple():
    try:
        from pro_football_reference_web_scraper import player_game_log
        
        print("Testing with simple call...")
        
        # Try with 2021 instead of 2022
        result = player_game_log.get_player_game_log(
            player='Patrick Mahomes', 
            position='QB', 
            season=2021
        )
        
        print(f"Result type: {type(result)}")
        print(f"Result: {result}")
        
        if result is not None:
            print("✅ Success!")
            if hasattr(result, 'shape'):
                print(f"Shape: {result.shape}")
            if hasattr(result, 'columns'):
                print(f"Columns: {list(result.columns)}")
            return True
        else:
            print("❌ Returned None")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_simple()