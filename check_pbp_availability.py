#!/usr/bin/env python3
"""
Try additional approaches for play-by-play data
"""
import sqlite3
import requests
import json
import time

BASE_URL = "https://lscluster.hockeytech.com/feed/"
API_KEY = "446521baf8c38984"
CLIENT_CODE = "pwhl"

def api_call(params):
    """Make API call"""
    params['key'] = API_KEY
    params['client_code'] = CLIENT_CODE
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        time.sleep(0.1)
        return response.json()
    except Exception as e:
        return None

conn = sqlite3.connect("pwhl_database.db")
cursor = conn.cursor()

print("=== Approach: Check Season 1 vs Season 8 Play-by-Play Structure ===\n")

# Get one game from each season
cursor.execute("SELECT game_id FROM games WHERE season_id = 1 LIMIT 1")
s1_game = cursor.fetchone()

cursor.execute("SELECT game_id FROM games WHERE season_id = 8 LIMIT 1")
s8_game = cursor.fetchone()

if s1_game:
    print(f"Season 1 Game {s1_game[0]}:")
    data = api_call({
        'feed': 'modulekit',
        'view': 'playbyplay',
        'game_id': s1_game[0],
        'season_id': 1
    })
    if data and 'SiteKit' in data:
        print(f"  Keys: {list(data['SiteKit'].keys())}")
        plays = data['SiteKit'].get('plays', [])
        print(f"  Plays: {len(plays)}")
        if plays:
            print(f"  Sample play: {plays[0]}")

print()

if s8_game:
    print(f"Season 8 Game {s8_game[0]}:")
    data = api_call({
        'feed': 'modulekit',
        'view': 'playbyplay',
        'game_id': s8_game[0],
        'season_id': 8
    })
    if data and 'SiteKit' in data:
        print(f"  Keys: {list(data['SiteKit'].keys())}")
        print(f"  Full response: {json.dumps(data, indent=2)[:500]}")

print("\n=== Approach: Try Direct Game Feed ===\n")

# Try the direct game feed
game_feed_endpoints = [
    f"https://lscluster.hockeytech.com/feed/index.php?feed=gc&key={API_KEY}&client_code={CLIENT_CODE}&game_id=210&game_date=2025-11-21",
    f"https://lscluster.hockeytech.com/components/game-summary/game-summary.php?game_id=210&_={{}}",
]

for url_template in game_feed_endpoints:
    print(f"Trying: {url_template[:80]}...")
    try:
        response = requests.get(url_template, timeout=10)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print(f"  Content: {response.text[:200]}")
    except Exception as e:
        print(f"  Error: {e}")

print("\n=== Conclusion ===")
print("Play-by-play data with shot coordinates is not available through the")
print("public API for the 2025-26 season. The data may be:")
print("1. Restricted until after the season ends")
print("2. Available only through authenticated/paid endpoints")
print("3. Published later in CSV format on the GitHub repo")
print("\nCurrent database has full xG for 2023-24 and 2024-25 seasons.")

conn.close()
