#!/usr/bin/env python3
"""
Verify expansion team player counts and check for missing players
"""
import sqlite3
import requests
import json
import time

BASE_URL = "https://lscluster.hockeytech.com/feed/"
API_KEY = "446521baf8c38984"
CLIENT_CODE = "pwhl"

def api_call(params):
    params['key'] = API_KEY
    params['client_code'] = CLIENT_CODE
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        time.sleep(0.2)
        return response.json()
    except Exception as e:
        print(f"API Error: {e}")
        return None

# Check Vancouver (team_id=9) and Seattle (team_id=8) roster sizes
for team_id, team_name in [(9, "Vancouver Goldeneyes"), (8, "Seattle Torrent")]:
    print(f"\n=== {team_name} (ID: {team_id}) ===")
    
    # Get roster from API
    data = api_call({
        'feed': 'modulekit',
        'view': 'roster',
        'team_id': team_id,
        'season_id': 8
    })
    
    if data and 'SiteKit' in data:
        roster = data['SiteKit'].get('Roster', [])
        print(f"Roster API: {len(roster)} players")
        
        # Show first 10 roster names
        for player in roster[:10]:
            print(f"  - {player.get('first_name')} {player.get('last_name')} (ID: {player.get('player_id')})")
    
    # Get stats from API
    data = api_call({
        'feed': 'modulekit',
        'view': 'statviewtype',
        'type': 'skaters',
        'league_id': 1,
        'team_id': team_id,
        'season_id': 8
    })
    
    if data and 'SiteKit' in data:
        stats = data['SiteKit'].get('Statviewtype', [])
        print(f"\nStats API: {len(stats)} players with stats")
        
        # Show first 10 stat leaders
        sorted_stats = sorted(stats, key=lambda x: int(x.get('points', 0)) if x.get('points') else 0, reverse=True)
        for stat in sorted_stats[:10]:
            print(f"  - {stat.get('first_name')} {stat.get('last_name')}: {stat.get('goals')}G, {stat.get('assists')}A, {stat.get('points')}PTS")

# Check database
print("\n=== Database Check ===")
conn = sqlite3.connect("pwhl_database.db")
cursor = conn.cursor()

for team_id, team_name in [(9, "Vancouver Goldeneyes"), (8, "Seattle Torrent")]:
    cursor.execute("SELECT COUNT(*) FROM skater_stats WHERE season_id = 8 AND team_id = ?", (team_id,))
    count = cursor.fetchone()[0]
    print(f"{team_name}: {count} skater stats in database")
    
    cursor.execute('''
        SELECT p.first_name, p.last_name, s.goals, s.assists, s.points 
        FROM skater_stats s 
        JOIN players p ON s.player_id = p.player_id 
        WHERE s.season_id = 8 AND s.team_id = ? 
        ORDER BY s.points DESC LIMIT 10
    ''', (team_id,))
    for row in cursor.fetchall():
        print(f"  - {row[0]} {row[1]}: {row[2]}G, {row[3]}A, {row[4]}PTS")

conn.close()
