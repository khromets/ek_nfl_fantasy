#!/usr/bin/env python3
"""
Debug player URL extraction from Pro Football Reference
Phase 1.2 - Step 3: Debug player URL extraction issue
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def debug_player_urls():
    """Debug why player URLs are not being extracted"""
    
    print("Debugging Player URL Extraction")
    print("=" * 40)
    
    # Test URL
    url = "https://www.pro-football-reference.com/years/2024/passing.htm"
    
    # Make request
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    
    print(f"Requesting: {url}")
    response = session.get(url, timeout=15)
    
    if response.status_code != 200:
        print(f"❌ Failed to get page: HTTP {response.status_code}")
        return
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the stats table
    stats_table = soup.find('table', {'class': 'stats_table'})
    if not stats_table:
        print("❌ No stats_table found")
        # Look for any table
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables total")
        if tables:
            stats_table = tables[0]
            print(f"Using first table with id: {stats_table.get('id', 'no id')}")
    
    if not stats_table:
        print("❌ No table found at all")
        return
    
    print(f"✅ Found stats table with id: {stats_table.get('id', 'no id')}")
    
    # Look for player links in different ways
    print("\n1. Looking for player links in table...")
    
    # Method 1: Look for all links in table
    all_links = stats_table.find_all('a')
    print(f"Total links in table: {len(all_links)}")
    
    player_links = []
    for link in all_links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        if '/players/' in href:
            player_links.append((text, href))
            print(f"  Player link: {text} -> {href}")
    
    print(f"Found {len(player_links)} player links")
    
    # Method 2: Look specifically in Player column
    print("\n2. Looking in header structure...")
    header_row = stats_table.find('thead')
    if header_row:
        headers = header_row.find_all('th')
        for i, header in enumerate(headers):
            header_text = header.get_text(strip=True)
            print(f"  Column {i}: {header_text}")
            if 'Player' in header_text:
                print(f"    --> Player column found at index {i}")
    
    # Method 3: Look at actual data rows
    print("\n3. Examining data rows...")
    tbody = stats_table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')[:3]  # First 3 rows
        for i, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            print(f"Row {i+1}:")
            for j, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                link = cell.find('a')
                link_href = link.get('href') if link else None
                if j < 5:  # First 5 columns
                    print(f"  Col {j}: '{text}' {f'(link: {link_href})' if link_href else ''}")
    
    # Method 4: Raw HTML inspection
    print("\n4. Raw HTML inspection of first few player cells...")
    tbody = stats_table.find('tbody')
    if tbody:
        first_row = tbody.find('tr')
        if first_row:
            cells = first_row.find_all(['td', 'th'])
            for i, cell in enumerate(cells[:5]):
                print(f"Cell {i} HTML: {str(cell)[:200]}...")
                print()

def test_manual_player_link():
    """Test extracting a specific player link manually"""
    print("\n" + "=" * 40)
    print("Testing Manual Player Link Extraction")
    print("=" * 40)
    
    # Use a known player URL
    test_url = "https://www.pro-football-reference.com/players/B/BurrJo01/gamelog/2024/"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    })
    
    print(f"Testing game log URL: {test_url}")
    response = session.get(test_url, timeout=15)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for game log table
        tables = soup.find_all('table')
        print(f"✅ Found {len(tables)} tables on game log page")
        
        if tables:
            game_table = tables[0]
            rows = game_table.find_all('tr')
            print(f"Game log has {len(rows)} rows")
            
            # Show first few games
            if len(rows) > 1:
                headers = [th.get_text(strip=True) for th in rows[0].find_all(['th', 'td'])]
                print(f"Headers: {headers[:10]}...")
                
                if len(rows) > 1:
                    first_game = [td.get_text(strip=True) for td in rows[1].find_all(['td', 'th'])]
                    print(f"First game: {first_game[:10]}...")
    else:
        print(f"❌ Failed to get game log: HTTP {response.status_code}")

def main():
    debug_player_urls()
    time.sleep(2)  # Be respectful
    test_manual_player_link()

if __name__ == "__main__":
    main()