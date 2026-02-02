#!/usr/bin/env python3
"""
Try Firebase API for PWHL play-by-play data
"""
import sqlite3
import requests
import json
import time

FIREBASE_URL = "https://leaguestat-b9523.firebaseio.com"

def try_endpoint(endpoint: str):
    """Try a Firebase endpoint"""
    try:
        url = f"{FIREBASE_URL}/{endpoint}.json"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data:
                return data
    except Exception as e:
        print(f"  Error: {e}")
    return None

# Try various Firebase endpoints
endpoints_to_try = [
    "pwhl",
    "pwhl/season_8",
    "pwhl/games",
    "pwhl/playbyplay",
    "games",
    "playbyplay",
    "events",
    "shots",
    "game_events",
]

print("=== Trying Firebase API Endpoints ===\n")

for endpoint in endpoints_to_try:
    print(f"Trying: {endpoint}")
    data = try_endpoint(endpoint)
    if data:
        print(f"  ✓ Success! Type: {type(data)}")
        if isinstance(data, dict):
            print(f"  Keys: {list(data.keys())[:10]}")
        print(f"  Preview: {str(data)[:200]}")
        print()
    else:
        print(f"  ✗ No data")

# Try with game ID
print("\n=== Trying with Game IDs ===")
conn = sqlite3.connect("pwhl_database.db")
cursor = conn.cursor()

cursor.execute("SELECT game_id FROM games WHERE season_id = 8 LIMIT 3")
games = cursor.fetchall()

for game_id_tuple in games:
    game_id = game_id_tuple[0]
    print(f"\nGame {game_id}:")
    
    game_endpoints = [
        f"pwhl/game_{game_id}",
        f"pwhl/games/{game_id}",
        f"games/{game_id}",
        f"game/{game_id}",
        f"events/{game_id}",
        f"playbyplay/{game_id}",
    ]
    
    for endpoint in game_endpoints:
        data = try_endpoint(endpoint)
        if data:
            print(f"  ✓ Found at: {endpoint}")
            print(f"  Data: {str(data)[:300]}")
            break
    else:
        print(f"  ✗ No play-by-play data found")

conn.close()
print("\n=== Done ===")
