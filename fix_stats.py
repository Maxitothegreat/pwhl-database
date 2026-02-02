#!/usr/bin/env python3
"""
Fix player stats - re-scrape skater and goalie stats for all teams and all seasons
"""
import sqlite3
import requests
import json
import time
from typing import Optional, Dict, List, Any

BASE_URL = "https://lscluster.hockeytech.com/feed/"
API_KEY = "446521baf8c38984"
CLIENT_CODE = "pwhl"

# All seasons including 2025-26
SEASONS = [1, 2, 3, 4, 5, 6, 7, 8]  # 2024 reg, 2024 pre, 2024 playoffs, 2024-25 pre, 2024-25 reg, 2025 playoffs, 2025-26 pre, 2025-26 reg

class StatsFixer:
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
            time.sleep(0.2)  # Rate limiting
            return response.json()
        except Exception as e:
            print(f"  API Error: {e}")
            return None
    
    def get_teams_from_db(self) -> List[Dict]:
        """Get teams from database"""
        self.cursor.execute("SELECT team_id, name FROM teams")
        teams = []
        for row in self.cursor.fetchall():
            teams.append({'id': row[0], 'name': row[1]})
        return teams
    
    def scrape_skater_stats_all_teams(self):
        """Scrape skater stats for all teams in each season"""
        print("\n=== Scraping Skater Stats (All Teams) ===")
        
        teams = self.get_teams_from_db()
        print(f"Found {len(teams)} teams in database")
        
        total_count = 0
        
        for season_id in SEASONS:
            print(f"\n  Season {season_id}:")
            season_count = 0
            
            for team in teams:
                team_id = team['id']
                team_name = team['name']
                
                # Fetch skater stats for this team/season
                data = self.api_call({
                    'feed': 'modulekit',
                    'view': 'statviewtype',
                    'type': 'skaters',
                    'league_id': 1,
                    'team_id': team_id,
                    'season_id': season_id
                })
                
                if not data or 'SiteKit' not in data:
                    continue
                
                stats = data['SiteKit'].get('Statviewtype', [])
                team_count = 0
                
                for stat in stats:
                    try:
                        player_id = int(stat.get('player_id', 0))
                        if player_id == 0:
                            continue
                        
                        # Parse ice time
                        ice_time_seconds = 0
                        if stat.get('ice_time'):
                            try:
                                parts = stat['ice_time'].split(':')
                                if len(parts) == 2:
                                    ice_time_seconds = int(parts[0]) * 60 + int(parts[1])
                            except:
                                pass
                        
                        self.cursor.execute('''
                            INSERT OR REPLACE INTO skater_stats 
                            (player_id, team_id, season_id, games_played, goals, assists, points,
                             points_per_game, shots, shooting_percentage, power_play_goals,
                             power_play_assists, power_play_points, short_handed_goals,
                             short_handed_assists, short_handed_points, shootout_goals,
                             shootout_attempts, shootout_percentage, shootout_games_played,
                             game_winning_goals, first_goals, insurance_goals, empty_net_goals,
                             overtime_goals, unassisted_goals, penalty_minutes, penalty_minutes_per_game,
                             minor_penalties, major_penalties, hits, hits_per_game,
                             shots_blocked_by_player, plus_minus, faceoff_attempts, faceoff_wins,
                             faceoff_percentage, ice_time_seconds, ice_time_avg_seconds, ice_time_per_game_avg)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            player_id, team_id, season_id,
                            int(stat.get('games_played', 0)) if stat.get('games_played') else 0,
                            int(stat.get('goals', 0)) if stat.get('goals') else 0,
                            int(stat.get('assists', 0)) if stat.get('assists') else 0,
                            int(stat.get('points', 0)) if stat.get('points') else 0,
                            float(stat.get('points_per_game', 0)) if stat.get('points_per_game') else 0,
                            int(stat.get('shots', 0)) if stat.get('shots') else 0,
                            float(stat.get('shooting_percentage', 0)) if stat.get('shooting_percentage') else 0,
                            int(stat.get('power_play_goals', 0)) if stat.get('power_play_goals') else 0,
                            int(stat.get('power_play_assists', 0)) if stat.get('power_play_assists') else 0,
                            int(stat.get('power_play_points', 0)) if stat.get('power_play_points') else 0,
                            int(stat.get('short_handed_goals', 0)) if stat.get('short_handed_goals') else 0,
                            int(stat.get('short_handed_assists', 0)) if stat.get('short_handed_assists') else 0,
                            int(stat.get('short_handed_points', 0)) if stat.get('short_handed_points') else 0,
                            int(stat.get('shootout_goals', 0)) if stat.get('shootout_goals') else 0,
                            int(stat.get('shootout_attempts', 0)) if stat.get('shootout_attempts') else 0,
                            float(stat.get('shootout_pct', 0)) if stat.get('shootout_pct') else 0,
                            int(stat.get('shootout_games_played', 0)) if stat.get('shootout_games_played') else 0,
                            int(stat.get('game_winning_goals', 0)) if stat.get('game_winning_goals') else 0,
                            int(stat.get('first_goals', 0)) if stat.get('first_goals') else 0,
                            int(stat.get('insurance_goals', 0)) if stat.get('insurance_goals') else 0,
                            int(stat.get('empty_net_goals', 0)) if stat.get('empty_net_goals') else 0,
                            int(stat.get('overtime_goals', 0)) if stat.get('overtime_goals') else 0,
                            int(stat.get('unassisted_goals', 0)) if stat.get('unassisted_goals') else 0,
                            int(stat.get('penalty_minutes', 0)) if stat.get('penalty_minutes') else 0,
                            float(stat.get('penalty_minutes_per_game', 0)) if stat.get('penalty_minutes_per_game') else 0,
                            int(stat.get('minor_penalties', 0)) if stat.get('minor_penalties') else 0,
                            int(stat.get('major_penalties', 0)) if stat.get('major_penalties') else 0,
                            int(stat.get('hits', 0)) if stat.get('hits') else 0,
                            float(stat.get('hits_per_game_avg', 0)) if stat.get('hits_per_game_avg') else 0,
                            int(stat.get('shots_blocked_by_player', 0)) if stat.get('shots_blocked_by_player') else 0,
                            int(stat.get('plus_minus', 0)) if stat.get('plus_minus') else 0,
                            int(stat.get('faceoff_attempts', 0)) if stat.get('faceoff_attempts') else 0,
                            int(stat.get('faceoff_wins', 0)) if stat.get('faceoff_wins') else 0,
                            float(stat.get('faceoff_pct', 0)) if stat.get('faceoff_pct') else 0,
                            ice_time_seconds,
                            int(float(stat['ice_time_avg'])) if stat.get('ice_time_avg') else 0,
                            stat.get('ice_time_per_game_avg')
                        ))
                        team_count += 1
                        season_count += 1
                        total_count += 1
                        
                    except Exception as e:
                        print(f"    Error: {e}")
                        continue
                
                if team_count > 0:
                    print(f"    {team_name}: {team_count} skaters")
            
            self.conn.commit()
            print(f"  Season {season_id} total: {season_count} skater records")
        
        print(f"\n✓ Total skater stats: {total_count}")
    
    def scrape_goalie_stats_all_teams(self):
        """Scrape goalie stats for all teams in each season"""
        print("\n=== Scraping Goalie Stats (All Teams) ===")
        
        teams = self.get_teams_from_db()
        total_count = 0
        
        for season_id in SEASONS:
            print(f"\n  Season {season_id}:")
            season_count = 0
            
            for team in teams:
                team_id = team['id']
                team_name = team['name']
                
                data = self.api_call({
                    'feed': 'modulekit',
                    'view': 'statviewtype',
                    'type': 'goalies',
                    'league_id': 1,
                    'team_id': team_id,
                    'season_id': season_id
                })
                
                if not data or 'SiteKit' not in data:
                    continue
                
                stats = data['SiteKit'].get('Statviewtype', [])
                team_count = 0
                
                for stat in stats:
                    try:
                        player_id = int(stat.get('player_id', 0))
                        if player_id == 0:
                            continue
                        
                        minutes_played = int(stat.get('minutes_played_g', 0)) if stat.get('minutes_played_g') else 0
                        seconds_played = int(stat.get('seconds_played', 0)) if stat.get('seconds_played') else 0
                        
                        self.cursor.execute('''
                            INSERT OR REPLACE INTO goalie_stats 
                            (player_id, team_id, season_id, games_played, minutes_played,
                             seconds_played, wins, losses, ot_losses, total_losses,
                             shutouts, saves, shots_against, goals_against,
                             empty_net_goals_against, save_percentage, goals_against_average,
                             shots_against_average, shootout_games_played, shootout_losses,
                             shootout_wins, shootout_goals_against, shootout_saves,
                             shootout_attempts, shootout_percentage, goals, assists,
                             points, penalty_minutes)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            player_id, team_id, season_id,
                            int(stat.get('games_played', 0)) if stat.get('games_played') else 0,
                            minutes_played,
                            seconds_played,
                            int(stat.get('wins', 0)) if stat.get('wins') else 0,
                            int(stat.get('losses', 0)) if stat.get('losses') else 0,
                            int(stat.get('ot_losses', 0)) if stat.get('ot_losses') else 0,
                            int(stat.get('total_losses', 0)) if stat.get('total_losses') else 0,
                            int(stat.get('shutouts', 0)) if stat.get('shutouts') else 0,
                            int(stat.get('saves', 0)) if stat.get('saves') else 0,
                            int(stat.get('shots', 0)) if stat.get('shots') else 0,
                            int(stat.get('goals_against', 0)) if stat.get('goals_against') else 0,
                            int(stat.get('empty_net_goals_against', 0)) if stat.get('empty_net_goals_against') else 0,
                            float(stat.get('save_percentage', 0)) if stat.get('save_percentage') else 0,
                            float(stat.get('goals_against_average', 0)) if stat.get('goals_against_average') else 0,
                            float(stat.get('shots_against_average', 0)) if stat.get('shots_against_average') else 0,
                            int(stat.get('shootout_games_played', 0)) if stat.get('shootout_games_played') else 0,
                            int(stat.get('shootout_losses', 0)) if stat.get('shootout_losses') else 0,
                            int(stat.get('shootout_wins', 0)) if stat.get('shootout_wins') else 0,
                            int(stat.get('shootout_goals_against', 0)) if stat.get('shootout_goals_against') else 0,
                            int(stat.get('shootout_saves', 0)) if stat.get('shootout_saves') else 0,
                            int(stat.get('shootout_attempts', 0)) if stat.get('shootout_attempts') else 0,
                            float(stat.get('shootout_percentage', 0)) if stat.get('shootout_percentage') else 0,
                            int(stat.get('goals', 0)) if stat.get('goals') else 0,
                            int(stat.get('assists', 0)) if stat.get('assists') else 0,
                            int(stat.get('points', 0)) if stat.get('points') else 0,
                            int(stat.get('penalty_minutes', 0)) if stat.get('penalty_minutes') else 0
                        ))
                        team_count += 1
                        season_count += 1
                        total_count += 1
                        
                    except Exception as e:
                        print(f"    Error: {e}")
                        continue
                
                if team_count > 0:
                    print(f"    {team_name}: {team_count} goalies")
            
            self.conn.commit()
            print(f"  Season {season_id} total: {season_count} goalie records")
        
        print(f"\n✓ Total goalie stats: {total_count}")
    
    def run(self):
        print("=== PWHL Stats Fix ===")
        print("Clearing old skater and goalie stats...")
        
        # Clear old stats
        self.cursor.execute("DELETE FROM skater_stats")
        self.cursor.execute("DELETE FROM goalie_stats")
        self.conn.commit()
        
        # Re-scrape
        self.scrape_skater_stats_all_teams()
        self.scrape_goalie_stats_all_teams()
        
        # Verify
        print("\n=== Verification ===")
        self.cursor.execute("SELECT COUNT(*) FROM skater_stats")
        skater_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM goalie_stats")
        goalie_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT season_id, COUNT(*) FROM skater_stats GROUP BY season_id ORDER BY season_id")
        skater_by_season = self.cursor.fetchall()
        
        print(f"Skater stats: {skater_count}")
        print(f"Goalie stats: {goalie_count}")
        print("\nSkater stats by season:")
        for season_id, count in skater_by_season:
            print(f"  Season {season_id}: {count}")
        
        # Show team breakdown for 2024-25 season
        print("\n2024-25 Regular Season skater stats by team:")
        self.cursor.execute('''
            SELECT t.name, COUNT(*) as count 
            FROM skater_stats s 
            JOIN teams t ON s.team_id = t.team_id 
            WHERE s.season_id = 5 
            GROUP BY t.name 
            ORDER BY count DESC
        ''')
        for row in self.cursor.fetchall():
            print(f"  {row[0]}: {row[1]}")
        
        self.conn.close()
        print("\n✓ Done!")

if __name__ == "__main__":
    fixer = StatsFixer()
    fixer.run()
