#!/usr/bin/env python3
"""
Scrape play-by-play data (shots) for 2025-26 season (season_id=8)
"""
import sqlite3
import requests
import json
import time
from typing import Optional, Dict, List, Any

BASE_URL = "https://lscluster.hockeytech.com/feed/"
API_KEY = "446521baf8c38984"
CLIENT_CODE = "pwhl"

class PlayByPlayScraper:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PWHL-Data-Scraper/1.0 (Research Project)'
        })
        
    def api_call(self, params: Dict[str, Any]) -> Optional[Dict]:
        """Make API call with rate limiting"""
        params['key'] = API_KEY
        params['client_code'] = CLIENT_CODE
        
        try:
            response = self.session.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            time.sleep(0.2)
            return response.json()
        except Exception as e:
            print(f"  API Error: {e}")
            return None
    
    def get_games_for_season(self, season_id: int) -> List[Dict]:
        """Get all games for a season"""
        data = self.api_call({
            'feed': 'modulekit',
            'view': 'schedule',
            'season_id': season_id
        })
        
        if not data or 'SiteKit' not in data:
            return []
        
        games = data['SiteKit'].get('Schedule', [])
        return [g for g in games if g.get('final') == '1']  # Only completed games
    
    def scrape_playbyplay(self, game_id: int, season_id: int) -> int:
        """Scrape play-by-play data for a game"""
        data = self.api_call({
            'feed': 'modulekit',
            'view': 'playbyplay',
            'game_id': game_id,
            'season_id': season_id
        })
        
        if not data or 'SiteKit' not in data:
            return 0
        
        plays = data['SiteKit'].get('plays', [])
        if not plays:
            plays = data['SiteKit'].get('Plays', [])
        
        shot_count = 0
        
        for play in plays:
            # Look for shot events
            event_type = play.get('event_type', '').lower()
            
            if 'shot' in event_type or 'goal' in event_type:
                try:
                    # Extract shot data
                    event_id = play.get('event_id', f"{game_id}_{shot_count}")
                    player_id = int(play.get('player_id', 0))
                    goalie_id = int(play.get('goalie_id', 0)) if play.get('goalie_id') else None
                    team_id = int(play.get('team_id', 0))
                    opponent_id = int(play.get('opponent_id', 0))
                    is_home = 1 if play.get('is_home', '0') == '1' else 0
                    period = int(play.get('period', 1))
                    time_formatted = play.get('time_formatted', '')
                    seconds = self._time_to_seconds(time_formatted)
                    
                    # Location
                    x = int(play.get('x_location', 0)) if play.get('x_location') else None
                    y = int(play.get('y_location', 0)) if play.get('y_location') else None
                    
                    # Shot info
                    shot_type = int(play.get('shot_type', 5))
                    shot_type_desc = play.get('shot_type_description', 'Default')
                    quality = int(play.get('shot_quality', 2))
                    quality_desc = play.get('shot_quality_description', 'Non quality on net')
                    is_goal = 1 if 'goal' in event_type else 0
                    game_goal_id = int(play.get('game_goal_id', 0)) if is_goal else None
                    
                    # Insert shot
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO shots 
                        (event_id, game_id, season_id, player_id, goalie_id, team_id, opponent_team_id,
                         is_home, period, time_formatted, seconds, x_location, y_location,
                         shot_type, shot_type_description, quality, shot_quality_description,
                         is_goal, game_goal_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        event_id, game_id, season_id, player_id, goalie_id, team_id, opponent_id,
                        is_home, period, time_formatted, seconds, x, y,
                        shot_type, shot_type_desc, quality, quality_desc,
                        is_goal, game_goal_id
                    ))
                    shot_count += 1
                    
                except Exception as e:
                    print(f"    Error processing shot: {e}")
                    continue
        
        return shot_count
    
    def _time_to_seconds(self, time_str: str) -> int:
        """Convert MM:SS time to seconds"""
        if not time_str or ':' not in time_str:
            return 0
        try:
            parts = time_str.split(':')
            return int(parts[0]) * 60 + int(parts[1])
        except:
            return 0
    
    def scrape_season_shots(self, season_id: int):
        """Scrape all shots for a season"""
        print(f"\n=== Scraping Shots for Season {season_id} ===")
        
        # Get games
        games = self.get_games_for_season(season_id)
        print(f"Found {len(games)} completed games")
        
        if not games:
            print("No games found")
            return
        
        total_shots = 0
        
        for i, game in enumerate(games):
            game_id = int(game.get('game_id', 0))
            if game_id == 0:
                continue
            
            home = game.get('home_team_name', '')
            away = game.get('visiting_team_name', '')
            print(f"  [{i+1}/{len(games)}] Game {game_id}: {away} @ {home}", end=" ")
            
            shots = self.scrape_playbyplay(game_id, season_id)
            total_shots += shots
            print(f"- {shots} shots")
            
            # Commit every 10 games
            if (i + 1) % 10 == 0:
                self.conn.commit()
        
        self.conn.commit()
        print(f"\n✓ Total shots scraped: {total_shots}")
    
    def run(self):
        print("=== PWHL Play-by-Play Scraper (2025-26 Season) ===")
        
        # Scrape season 8 (2025-26)
        self.scrape_season_shots(8)
        
        # Verify
        print("\n=== Verification ===")
        self.cursor.execute("SELECT season_id, COUNT(*) FROM shots GROUP BY season_id ORDER BY season_id")
        for row in self.cursor.fetchall():
            print(f"  Season {row[0]}: {row[1]} shots")
        
        self.conn.close()
        print("\n✓ Done!")

if __name__ == "__main__":
    scraper = PlayByPlayScraper()
    scraper.run()
