#!/usr/bin/env python3
"""
Add expansion teams (Seattle, Vancouver) for 2025-26 season
"""
import sqlite3
import requests
import json
import time
from typing import Optional, Dict, List, Any

BASE_URL = "https://lscluster.hockeytech.com/feed/"
API_KEY = "446521baf8c38984"
CLIENT_CODE = "pwhl"

class ExpansionTeamScraper:
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
    
    def get_teams_from_schedule(self, season_id: int) -> List[Dict]:
        """Get teams from schedule data"""
        data = self.api_call({
            'feed': 'modulekit',
            'view': 'schedule',
            'season_id': season_id
        })
        
        if not data or 'SiteKit' not in data:
            return []
        
        teams = {}
        games = data['SiteKit'].get('Schedule', [])
        
        for game in games:
            # Home team
            home_id = int(game.get('home_team', 0))
            if home_id > 0 and home_id not in teams:
                teams[home_id] = {
                    'id': home_id,
                    'name': game.get('home_team_name', ''),
                    'nickname': game.get('home_team_nickname', ''),
                    'code': game.get('home_team_code', ''),
                    'city': game.get('home_team_city', '')
                }
            
            # Away team
            away_id = int(game.get('visiting_team', 0))
            if away_id > 0 and away_id not in teams:
                teams[away_id] = {
                    'id': away_id,
                    'name': game.get('visiting_team_name', ''),
                    'nickname': game.get('visiting_team_nickname', ''),
                    'code': game.get('visiting_team_code', ''),
                    'city': game.get('visiting_team_city', '')
                }
        
        return list(teams.values())
    
    def add_expansion_teams(self):
        """Add Seattle and Vancouver teams"""
        print("=== Adding Expansion Teams ===\n")
        
        expansion_teams = []
        
        for season_id in [7, 8]:
            teams = self.get_teams_from_schedule(season_id)
            print(f"Season {season_id}: Found {len(teams)} teams")
            
            for team in teams:
                team_id = team['id']
                
                # Check if team exists in DB
                self.cursor.execute("SELECT team_id FROM teams WHERE team_id = ?", (team_id,))
                if not self.cursor.fetchone():
                    print(f"  New team: {team['name']} (ID: {team_id}, Code: {team['code']})")
                    expansion_teams.append(team)
                    
                    # Add to database
                    self.cursor.execute('''
                        INSERT INTO teams (team_id, name, nickname, code, city, logo_url)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        team_id,
                        team['name'],
                        team['nickname'],
                        team['code'],
                        team['city'],
                        ''
                    ))
        
        self.conn.commit()
        
        # Show all teams now
        print("\n=== All Teams in Database ===")
        self.cursor.execute("SELECT team_id, name, code FROM teams ORDER BY team_id")
        for row in self.cursor.fetchall():
            print(f"  {row[0]}: {row[1]} ({row[2]})")
        
        return len(expansion_teams)
    
    def scrape_expansion_team_stats(self):
        """Scrape stats for expansion teams in seasons 7 and 8"""
        print("\n=== Scraping Stats for Expansion Teams ===")
        
        # Get expansion teams (IDs 8 and 9)
        self.cursor.execute("SELECT team_id, name FROM teams WHERE team_id IN (8, 9)")
        expansion_teams = self.cursor.fetchall()
        
        if not expansion_teams:
            print("No expansion teams found")
            return
        
        print(f"Found {len(expansion_teams)} expansion teams:")
        for team_id, team_name in expansion_teams:
            print(f"  {team_id}: {team_name}")
        
        # Scrape skater stats for seasons 7 and 8
        total_added = 0
        
        for season_id in [7, 8]:
            print(f"\nSeason {season_id}:")
            
            for team_id, team_name in expansion_teams:
                # Skater stats
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
                count = 0
                
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
                        count += 1
                        total_added += 1
                        
                    except Exception as e:
                        print(f"    Error: {e}")
                        continue
                
                if count > 0:
                    print(f"  {team_name}: {count} skaters")
                
                # Goalie stats
                data = self.api_call({
                    'feed': 'modulekit',
                    'view': 'statviewtype',
                    'type': 'goalies',
                    'league_id': 1,
                    'team_id': team_id,
                    'season_id': season_id
                })
                
                if data and 'SiteKit' in data:
                    stats = data['SiteKit'].get('Statviewtype', [])
                    goalie_count = 0
                    
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
                            goalie_count += 1
                            
                        except Exception as e:
                            print(f"    Goalie error: {e}")
                            continue
                    
                    if goalie_count > 0:
                        print(f"  {team_name}: {goalie_count} goalies")
                
                self.conn.commit()
        
        print(f"\n✓ Added {total_added} expansion team player records")
    
    def run(self):
        print("=== Adding Expansion Teams (Seattle, Vancouver) ===\n")
        
        # Add teams
        new_teams = self.add_expansion_teams()
        
        # Scrape stats for expansion teams
        if new_teams > 0:
            self.scrape_expansion_team_stats()
        
        # Final verification
        print("\n=== Final Verification ===")
        self.cursor.execute("SELECT COUNT(*) FROM teams")
        team_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT season_id, COUNT(*) FROM skater_stats GROUP BY season_id ORDER BY season_id")
        by_season = self.cursor.fetchall()
        
        print(f"Total teams: {team_count}")
        print("\nSkater stats by season:")
        for season_id, count in by_season:
            print(f"  Season {season_id}: {count}")
        
        # Show team breakdown for expansion seasons
        print("\n=== 2025-26 Regular Season (Season 8) Team Breakdown ===")
        self.cursor.execute('''
            SELECT t.name, COUNT(*) as count 
            FROM skater_stats s 
            JOIN teams t ON s.team_id = t.team_id 
            WHERE s.season_id = 8 
            GROUP BY t.name 
            ORDER BY count DESC
        ''')
        for row in self.cursor.fetchall():
            print(f"  {row[0]}: {row[1]} skaters")
        
        self.conn.close()
        print("\n✓ Done!")

if __name__ == "__main__":
    scraper = ExpansionTeamScraper()
    scraper.run()
