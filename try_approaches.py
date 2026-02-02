#!/usr/bin/env python3
"""
Try different approaches to get 2025-26 play-by-play data
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
        time.sleep(0.2)
        return response.json()
    except Exception as e:
        return None

conn = sqlite3.connect("pwhl_database.db")
cursor = conn.cursor()

# First, let's scrape the games for season 8
print("=== Scraping Games for Season 8 ===\n")

data = api_call({
    'feed': 'modulekit',
    'view': 'schedule',
    'season_id': 8
})

games_to_add = []
if data and 'SiteKit' in data:
    games = data['SiteKit'].get('Schedule', [])
    print(f"Found {len(games)} games in schedule")
    
    for game in games:
        if game.get('final') == '1':
            game_id = int(game.get('game_id', 0))
            if game_id > 0:
                games_to_add.append({
                    'game_id': game_id,
                    'date': game.get('date_time_played'),
                    'home_team': int(game.get('home_team', 0)),
                    'away_team': int(game.get('visiting_team', 0)),
                    'home_goals': int(game.get('home_goal_count', 0)),
                    'away_goals': int(game.get('visiting_goal_count', 0)),
                    'status': game.get('game_status', '')
                })

print(f"Found {len(games_to_add)} completed games")

if games_to_add:
    # Add games to database
    for g in games_to_add:
        cursor.execute('''
            INSERT OR REPLACE INTO games 
            (game_id, season_id, date_played, home_team_id, away_team_id, home_goals, away_goals, game_status)
            VALUES (?, 8, ?, ?, ?, ?, ?, ?)
        ''', (g['game_id'], g['date'], g['home_team'], g['away_team'], 
              g['home_goals'], g['away_goals'], g['status']))
    conn.commit()
    print(f"✓ Added {len(games_to_add)} games to database")

# Now try different play-by-play approaches
print("\n=== Approach 1: Try Different Play-by-Play Views ===\n")

if games_to_add:
    game_id = games_to_add[0]['game_id']
    print(f"Testing with Game ID: {game_id}\n")
    
    views_to_try = [
        'playbyplay',
        'playbyplay_l',
        'pbp',
        'plays',
        'game_summary',
        'game_summary_l',
    ]
    
    for view in views_to_try:
        print(f"Trying view: {view}")
        data = api_call({
            'feed': 'modulekit',
            'view': view,
            'game_id': game_id,
            'season_id': 8
        })
        
        if data and 'SiteKit' in data:
            print(f"  ✓ Got data!")
            keys = list(data['SiteKit'].keys()) if isinstance(data['SiteKit'], dict) else 'Not a dict'
            print(f"  Keys: {keys}")
            
            # Check for plays
            plays = data['SiteKit'].get('plays', []) or data['SiteKit'].get('Plays', [])
            print(f"  Plays count: {len(plays)}")
            
            if plays:
                print(f"  Sample play keys: {list(plays[0].keys()) if isinstance(plays[0], dict) else 'Not a dict'}")
        else:
            print(f"  ✗ No data")

print("\n=== Done ===")
conn.close()
