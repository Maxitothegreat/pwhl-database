#!/usr/bin/env python3
"""
PWHL Database Scraper
Scrapes data from HockeyTech/LeagueStat API for the Professional Women's Hockey League

Data sources:
- HockeyTech API (lscluster.hockeytech.com)
- GitHub: IsabelleLefebvre97/PWHL-Data-Reference (play-by-play CSVs)

Seasons covered:
- 2023-24 (season_id=1)
- 2024-25 (season_id=5)
"""

import sqlite3
import requests
import json
import time
import csv
import io
from datetime import datetime
from typing import Optional, Dict, List, Any

# API Configuration
BASE_URL = "https://lscluster.hockeytech.com/feed/"
API_KEY = "446521baf8c38984"
CLIENT_CODE = "pwhl"

# GitHub CSV data
GITHUB_RAW = "https://raw.githubusercontent.com/IsabelleLefebvre97/PWHL-Data-Reference/main/data"

# Seasons to scrape
SEASONS = [1, 5]  # 2023-24, 2024-25


class PWHLScraper:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PWHL-Data-Scraper/1.0 (Research Project)'
        })
        
    def init_database(self):
        """Initialize database from schema.sql"""
        with open('schema.sql', 'r') as f:
            schema = f.read()
        self.cursor.executescript(schema)
        self.conn.commit()
        print("✓ Database initialized from schema.sql")
        
    def api_call(self, params: Dict[str, Any]) -> Optional[Dict]:
        """Make API call with rate limiting and error handling"""
        params['key'] = API_KEY
        params['client_code'] = CLIENT_CODE
        
        try:
            response = self.session.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            time.sleep(0.1)  # Rate limiting
            return response.json()
        except Exception as e:
            print(f"  API Error: {e}")
            return None
    
    def fetch_csv(self, url: str) -> Optional[List[Dict]]:
        """Fetch CSV from GitHub and return as list of dicts"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            content = response.content.decode('utf-8-sig')  # Handle BOM
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
        except Exception as e:
            print(f"  CSV Error: {e}")
            return None
    
    # =====================================================
    # SEASONS & TEAMS
    # =====================================================
    
    def scrape_seasons(self):
        """Scrape all seasons"""
        print("\n=== Scraping Seasons ===")
        data = self.api_call({'feed': 'modulekit', 'view': 'seasons'})
        
        if not data or 'SiteKit' not in data:
            print("  No seasons data found")
            return
            
        seasons = data['SiteKit'].get('Seasons', [])
        
        for season in seasons:
            self.cursor.execute('''
                INSERT OR REPLACE INTO seasons 
                (season_id, season_name, shortname, career, playoff, start_date, end_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                int(season['season_id']),
                season['season_name'],
                season['shortname'],
                int(season['career']),
                int(season['playoff']),
                season['start_date'],
                season['end_date']
            ))
        
        self.conn.commit()
        print(f"✓ Scraped {len(seasons)} seasons")
    
    def scrape_teams(self):
        """Scrape teams from all seasons"""
        print("\n=== Scraping Teams ===")
        
        for season_id in SEASONS:
            data = self.api_call({
                'feed': 'modulekit',
                'view': 'teamsbyseason',
                'season_id': season_id
            })
            
            if not data or 'SiteKit' not in data:
                continue
                
            teams = data['SiteKit'].get('Teamsbyseason', [])
            
            for team in teams:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO teams 
                    (team_id, name, nickname, code, city, logo_url, league_id, division_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    int(team['id']),
                    team['name'],
                    team['nickname'],
                    team['code'],
                    team['city'],
                    team.get('team_logo_url', ''),
                    int(team.get('division_id', 1)),
                    int(team.get('division_id', 1))
                ))
        
        self.conn.commit()
        print(f"✓ Scraped teams")
    
    # =====================================================
    # PLAYERS & ROSTERS
    # =====================================================
    
    def scrape_players(self):
        """Scrape players from all_players.csv and API"""
        print("\n=== Scraping Players ===")
        
        # First get from CSV (most complete player list)
        csv_data = self.fetch_csv(f"{GITHUB_RAW}/players/all_players.csv")
        
        if csv_data:
            for player in csv_data:
                player_id = int(player['id'])
                team_id = int(player['team_id']) if player.get('team_id') else None
                
                self.cursor.execute('''
                    INSERT OR REPLACE INTO players 
                    (player_id, first_name, last_name, full_name, position, position_analysis,
                     shoots, catches, height, weight, birthdate, hometown, home_province,
                     home_country, nationality, player_image_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_id,
                    player['first_name'],
                    player['last_name'],
                    f"{player['first_name']} {player['last_name']}",
                    player.get('position', ''),
                    player.get('position_analysis', ''),
                    player.get('shoots'),
                    player.get('catches'),
                    player.get('height'),
                    None,  # weight not in CSV
                    player.get('birthdate') if player.get('birthdate') else None,
                    player.get('hometown'),
                    player.get('hometown_div'),
                    None,  # home_country
                    player.get('nationality'),
                    player.get('image')
                ))
            
            self.conn.commit()
            print(f"✓ Scraped {len(csv_data)} players from CSV")
        
        # Also get from API rosters for additional details
        for season_id in SEASONS:
            for team_id in range(1, 7):  # Teams 1-6
                data = self.api_call({
                    'feed': 'modulekit',
                    'view': 'roster',
                    'team_id': team_id,
                    'season_id': season_id
                })
                
                if not data or 'SiteKit' not in data:
                    continue
                    
                roster = data['SiteKit'].get('Roster', [])
                
                for player in roster:
                    # Skip if player is not a dict (sometimes API returns lists)
                    if not isinstance(player, dict):
                        continue
                    
                    try:
                        player_id = int(player.get('player_id', 0))
                        if player_id == 0:
                            continue
                        
                        # Update player info if exists, or insert
                        self.cursor.execute('''
                            INSERT OR REPLACE INTO players 
                            (player_id, first_name, last_name, full_name, position, 
                             shoots, height, weight, birthdate, hometown, home_province,
                             home_country, player_image_url, active)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            player_id,
                            player.get('first_name', ''),
                            player.get('last_name', ''),
                            f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
                            player.get('position', ''),
                            player.get('shoots'),
                            player.get('height'),
                            None,  # weight
                            player.get('birthdate') if player.get('birthdate') else None,
                            player.get('hometown'),
                            player.get('homeprov'),
                            player.get('birthcntry'),
                            player.get('player_image', ''),
                            1 if player.get('active') == '1' else 0
                        ))
                        
                        # Insert roster assignment
                        self.cursor.execute('''
                            INSERT OR REPLACE INTO roster_assignments
                            (player_id, team_id, season_id, jersey_number, rookie, veteran, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                        player_id,
                        team_id,
                        season_id,
                        int(player.get('tp_jersey_number')) if player.get('tp_jersey_number') else None,
                        1 if player.get('rookie') == '1' else 0,
                        int(player.get('veteran_status')) if player.get('veteran_status') else 0,
                        player.get('status')
                        ))
                    except Exception as e:
                        # Skip players that can't be processed
                        continue
        
        self.conn.commit()
        print("✓ Scraped roster assignments from API")
    
    # =====================================================
    # GAMES
    # =====================================================
    
    def scrape_games(self):
        """Scrape games from schedule API and CSV"""
        print("\n=== Scraping Games ===")
        
        count = 0
        
        # Get from API for each season
        for season_id in SEASONS:
            data = self.api_call({
                'feed': 'modulekit',
                'view': 'schedule',
                'season_id': season_id
            })
            
            if not data or 'SiteKit' not in data:
                continue
                
            games = data['SiteKit'].get('Schedule', [])
            
            for game in games:
                try:
                    date_played = game.get('date_time_played')
                    if date_played:
                        # Parse ISO format
                        date_played = date_played.replace('Z', '+00:00')
                    
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO games 
                        (game_id, season_id, game_number, date_played, timezone,
                         home_team_id, away_team_id, home_goals, away_goals,
                         periods, overtime, shootout, game_status, venue_name,
                         venue_location, attendance)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        int(game['game_id']),
                        int(game['season_id']),
                        int(game['game_number']),
                        date_played,
                        game.get('timezone'),
                        int(game['home_team']),
                        int(game['visiting_team']),
                        int(game.get('home_goal_count', 0)),
                        int(game.get('visiting_goal_count', 0)),
                        int(game.get('period', 3)),
                        1 if game.get('overtime') == '1' else 0,
                        1 if game.get('shootout') == '1' else 0,
                        game.get('game_status'),
                        game.get('venue_name'),
                        game.get('venue_location'),
                        int(game['attendance']) if game.get('attendance') else None
                    ))
                    count += 1
                except Exception as e:
                    print(f"  Error processing game {game.get('game_id')}: {e}")
        
        self.conn.commit()
        print(f"✓ Scraped {count} games")
    
    # =====================================================
    # PLAYER STATISTICS
    # =====================================================
    
    def scrape_skater_stats(self):
        """Scrape skater statistics for all seasons"""
        print("\n=== Scraping Skater Statistics ===")
        
        count = 0
        
        for season_id in SEASONS:
            data = self.api_call({
                'feed': 'modulekit',
                'view': 'statviewtype',
                'type': 'skaters',
                'league_id': 1,
                'team_id': 0,  # All teams
                'season_id': season_id
            })
            
            if not data or 'SiteKit' not in data:
                continue
                
            stats = data['SiteKit'].get('Statviewtype', [])
            
            for stat in stats:
                try:
                    player_id = int(stat['player_id'])
                    team_id = int(stat['team_id'])
                    
                    # Parse ice time
                    ice_time_seconds = int(stat['ice_time']) if stat.get('ice_time') else 0
                    
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
                         faceoff_percentage, ice_time_seconds, ice_time_avg_seconds,
                         ice_time_per_game_avg)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        player_id, team_id, season_id,
                        int(stat.get('games_played', 0)),
                        int(stat.get('goals', 0)),
                        int(stat.get('assists', 0)),
                        int(stat.get('points', 0)),
                        float(stat.get('points_per_game', 0)) if stat.get('points_per_game') else 0,
                        int(stat.get('shots', 0)),
                        float(stat.get('shooting_percentage', 0)) if stat.get('shooting_percentage') else 0,
                        int(stat.get('power_play_goals', 0)),
                        int(stat.get('power_play_assists', 0)),
                        int(stat.get('power_play_points', 0)),
                        int(stat.get('short_handed_goals', 0)),
                        int(stat.get('short_handed_assists', 0)),
                        int(stat.get('short_handed_points', 0)),
                        int(stat.get('shootout_goals', 0)),
                        int(stat.get('shootout_attempts', 0)),
                        float(stat.get('shootout_percentage', 0)) if stat.get('shootout_percentage') else 0,
                        int(stat.get('shootout_games_played', 0)),
                        int(stat.get('game_winning_goals', 0)),
                        int(stat.get('first_goals', 0)),
                        int(stat.get('insurance_goals', 0)),
                        int(stat.get('empty_net_goals', 0)),
                        int(stat.get('overtime_goals', 0)),
                        int(stat.get('unassisted_goals', 0)),
                        int(stat.get('penalty_minutes', 0)),
                        float(stat.get('penalty_minutes_per_game', 0)) if stat.get('penalty_minutes_per_game') else 0,
                        int(stat.get('minor_penalties', 0)),
                        int(stat.get('major_penalties', 0)),
                        int(stat.get('hits', 0)),
                        float(stat.get('hits_per_game_avg', 0)) if stat.get('hits_per_game_avg') else 0,
                        int(stat.get('shots_blocked_by_player', 0)),
                        int(stat.get('plus_minus', 0)),
                        int(stat.get('faceoff_attempts', 0)),
                        int(stat.get('faceoff_wins', 0)),
                        float(stat.get('faceoff_pct', 0)) if stat.get('faceoff_pct') else 0,
                        ice_time_seconds,
                        int(float(stat['ice_time_avg'])) if stat.get('ice_time_avg') else 0,
                        stat.get('ice_time_per_game_avg')
                    ))
                    count += 1
                except Exception as e:
                    print(f"  Error processing skater stat for player {stat.get('player_id')}: {e}")
        
        self.conn.commit()
        print(f"✓ Scraped {count} skater stat records")
    
    def scrape_goalie_stats(self):
        """Scrape goalie statistics for all seasons"""
        print("\n=== Scraping Goalie Statistics ===")
        
        count = 0
        
        for season_id in SEASONS:
            data = self.api_call({
                'feed': 'modulekit',
                'view': 'statviewtype',
                'type': 'goalies',
                'league_id': 1,
                'team_id': 0,
                'season_id': season_id
            })
            
            if not data or 'SiteKit' not in data:
                continue
                
            stats = data['SiteKit'].get('Statviewtype', [])
            
            for stat in stats:
                try:
                    player_id = int(stat['player_id'])
                    team_id = int(stat['team_id'])
                    
                    # Parse minutes
                    minutes_played = 0
                    if stat.get('minutes_played_g'):
                        minutes_played = int(stat['minutes_played_g'])
                    
                    seconds_played = int(stat.get('seconds_played', 0))
                    
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
                        int(stat.get('games_played', 0)),
                        minutes_played,
                        seconds_played,
                        int(stat.get('wins', 0)),
                        int(stat.get('losses', 0)),
                        int(stat.get('ot_losses', 0)),
                        int(stat.get('total_losses', 0)),
                        int(stat.get('shutouts', 0)),
                        int(stat.get('saves', 0)),
                        int(stat.get('shots', 0)),
                        int(stat.get('goals_against', 0)),
                        int(stat.get('empty_net_goals_against', 0)),
                        float(stat.get('save_percentage', 0)) if stat.get('save_percentage') else 0,
                        float(stat.get('goals_against_average', 0)) if stat.get('goals_against_average') else 0,
                        float(stat.get('shots_against_average', 0)) if stat.get('shots_against_average') else 0,
                        int(stat.get('shootout_games_played', 0)),
                        int(stat.get('shootout_losses', 0)),
                        int(stat.get('shootout_wins', 0)),
                        int(stat.get('shootout_goals_against', 0)),
                        int(stat.get('shootout_saves', 0)),
                        int(stat.get('shootout_attempts', 0)),
                        float(stat.get('shootout_percentage', 0)) if stat.get('shootout_percentage') else 0,
                        int(stat.get('goals', 0)),
                        int(stat.get('assists', 0)),
                        int(stat.get('points', 0)),
                        int(stat.get('penalty_minutes', 0))
                    ))
                    count += 1
                except Exception as e:
                    print(f"  Error processing goalie stat for player {stat.get('player_id')}: {e}")
        
        self.conn.commit()
        print(f"✓ Scraped {count} goalie stat records")
    
    # =====================================================
    # TEAM STATISTICS
    # =====================================================
    
    def scrape_team_stats(self):
        """Scrape team standings/statistics"""
        print("\n=== Scraping Team Statistics ===")
        
        count = 0
        
        for season_id in SEASONS:
            data = self.api_call({
                'feed': 'modulekit',
                'view': 'statviewtype',
                'stat': 'conference',
                'type': 'standings',
                'season_id': season_id
            })
            
            if not data or 'SiteKit' not in data:
                continue
                
            stats = data['SiteKit'].get('Statviewtype', [])
            
            for stat in stats:
                try:
                    if 'team_id' not in stat:
                        continue  # Skip header rows
                    
                    team_id = int(stat['team_id'])
                    
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO team_stats 
                        (team_id, season_id, games_played, wins, losses, ot_losses,
                         shootout_wins, shootout_losses, points, win_percentage,
                         percentage, goals_for, goals_against, goals_diff,
                         power_plays, power_play_goals, power_play_percentage,
                         times_short_handed, power_play_goals_against, penalty_kill_percentage,
                         short_handed_goals_for, short_handed_goals_against,
                         shootout_goals, shootout_goals_against, shootout_attempts,
                         shootout_attempts_against, shootout_games_played, shootout_percentage,
                         penalty_minutes, pim_per_game, streak, past_10, home_record,
                         away_record, shootout_record, overall_rank, division_rank,
                         clinched_playoff_spot)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        team_id, season_id,
                        int(stat.get('games_played', 0)),
                        int(stat.get('wins', 0)),
                        int(stat.get('losses', 0)),
                        int(stat.get('ot_losses', 0)),
                        int(stat.get('shootout_wins', 0)),
                        int(stat.get('shootout_losses', 0)),
                        int(stat.get('points', 0)),
                        float(stat.get('win_percentage', 0)) if stat.get('win_percentage') else 0,
                        float(stat.get('percentage', 0)) if stat.get('percentage') else 0,
                        int(stat.get('goals_for', 0)),
                        int(stat.get('goals_against', 0)),
                        int(stat.get('goals_diff', 0)),
                        int(stat.get('power_plays', 0)),
                        int(stat.get('power_play_goals', 0)),
                        float(stat.get('power_play_pct', 0)) if stat.get('power_play_pct') else 0,
                        int(stat.get('times_short_handed', 0)),
                        int(stat.get('power_play_goals_against', 0)),
                        float(stat.get('penalty_kill_pct', 0)) if stat.get('penalty_kill_pct') else 0,
                        int(stat.get('short_handed_goals_for', 0)),
                        int(stat.get('short_handed_goals_against', 0)),
                        int(stat.get('shootout_goals', 0)),
                        int(stat.get('shootout_goals_against', 0)),
                        int(stat.get('shootout_attempts', 0)),
                        int(stat.get('shootout_attempts_against', 0)),
                        int(stat.get('shootout_games_played', 0)),
                        float(stat.get('shootout_pct', 0)) if stat.get('shootout_pct') else 0,
                        int(stat.get('penalty_minutes', 0)),
                        float(stat.get('pim_pg', 0)) if stat.get('pim_pg') else 0,
                        stat.get('streak'),
                        stat.get('past_10'),
                        stat.get('home_record'),
                        stat.get('visiting_record'),
                        stat.get('shootout_record'),
                        int(stat.get('overall_rank', 0)),
                        int(stat.get('rank', 0)),
                        1 if stat.get('clinched_playoff_spot') == '1' else 0
                    ))
                    count += 1
                except Exception as e:
                    print(f"  Error processing team stat for team {stat.get('team_id')}: {e}")
        
        self.conn.commit()
        print(f"✓ Scraped {count} team stat records")
    
    # =====================================================
    # PLAY-BY-PLAY DATA FROM CSV
    # =====================================================
    
    def scrape_play_by_play(self):
        """Scrape play-by-play data from GitHub CSVs"""
        print("\n=== Scraping Play-by-Play Data ===")
        
        # Shots
        print("  Fetching shots...")
        shots = self.fetch_csv(f"{GITHUB_RAW}/games/play_by_play/shots.csv")
        if shots:
            for shot in shots:
                try:
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO shots 
                        (event_id, game_id, season_id, player_id, goalie_id, team_id,
                         opponent_team_id, is_home, period, time_formatted, seconds,
                         x_location, y_location, shot_type, shot_type_description,
                         quality, shot_quality_description, is_goal, game_goal_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        shot.get('id'),
                        int(shot['game_id']),
                        int(shot['season_id']),
                        int(shot['player_id']),
                        int(shot['goalie_id']) if shot.get('goalie_id') else None,
                        int(shot['team_id']),
                        int(shot['opponent_team_id']),
                        1 if shot.get('home') == '1' else 0,
                        int(shot['period']),
                        shot.get('time_formatted'),
                        int(shot['seconds']),
                        int(shot['x_location']) if shot.get('x_location') else None,
                        int(shot['y_location']) if shot.get('y_location') else None,
                        shot.get('shot_type'),
                        shot.get('shot_type_description'),
                        int(shot['quality']) if shot.get('quality') else None,
                        shot.get('shot_quality_description'),
                        1 if shot.get('game_goal_id') else 0,
                        shot.get('game_goal_id') if shot.get('game_goal_id') else None
                    ))
                except Exception as e:
                    pass  # Skip problematic rows
            self.conn.commit()
            print(f"    ✓ {len(shots)} shots")
        
        # Goals
        print("  Fetching goals...")
        goals = self.fetch_csv(f"{GITHUB_RAW}/games/play_by_play/goals.csv")
        if goals:
            for goal in goals:
                try:
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO goals 
                        (event_id, game_id, season_id, goal_id, team_id, scorer_id,
                         assist1_id, assist2_id, opponent_team_id, is_home,
                         period, time_formatted, seconds, x_location, y_location,
                         goal_type, power_play, short_handed, empty_net, game_winning, insurance_goal)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        goal.get('id'),
                        int(goal['game_id']),
                        int(goal['season_id']),
                        int(goal['event_id']) if goal.get('event_id') else None,
                        int(goal['team_id']),
                        int(goal['goal_player_id']),
                        int(goal['assist1_player_id']) if goal.get('assist1_player_id') else None,
                        int(goal['assist2_player_id']) if goal.get('assist2_player_id') else None,
                        int(goal['opponent_team_id']),
                        1 if goal.get('home') == '1' else 0,
                        int(goal['period']),
                        goal.get('time_formatted'),
                        int(goal['seconds']),
                        int(goal['x_location']) if goal.get('x_location') else None,
                        int(goal['y_location']) if goal.get('y_location') else None,
                        goal.get('goal_type'),
                        1 if goal.get('power_play') == '1' else 0,
                        1 if goal.get('short_handed') == '1' else 0,
                        1 if goal.get('empty_net') == '1' else 0,
                        1 if goal.get('game_winning') == '1' else 0,
                        1 if goal.get('insurance_goal') == '1' else 0
                    ))
                except Exception as e:
                    pass
            self.conn.commit()
            print(f"    ✓ {len(goals)} goals")
        
        # Penalties
        print("  Fetching penalties...")
        penalties = self.fetch_csv(f"{GITHUB_RAW}/games/play_by_play/penalties.csv")
        if penalties:
            for pen in penalties:
                try:
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO penalties 
                        (event_id, game_id, season_id, player_id, team_id, is_home,
                         period, time_formatted, seconds, minutes, penalty_class,
                         penalty_description, is_bench, is_penalty_shot, pp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        pen.get('id'),
                        int(pen['game_id']),
                        int(pen['season_id']),
                        int(pen['player_id']),
                        int(pen['team_id']),
                        1 if pen.get('home') == '1' else 0,
                        int(pen['period']),
                        pen.get('time_off_formatted'),
                        None,  # seconds not in CSV
                        int(float(pen['minutes'])) if pen.get('minutes') else 0,
                        pen.get('penalty_class'),
                        pen.get('lang_penalty_description'),
                        1 if pen.get('bench') == '1' else 0,
                        1 if pen.get('penalty_shot') == '1' else 0,
                        1 if pen.get('pp') == '1' else 0
                    ))
                except Exception as e:
                    pass
            self.conn.commit()
            print(f"    ✓ {len(penalties)} penalties")
        
        # Faceoffs
        print("  Fetching faceoffs...")
        faceoffs = self.fetch_csv(f"{GITHUB_RAW}/games/play_by_play/faceoffs.csv")
        if faceoffs:
            for faceoff in faceoffs:
                try:
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO faceoffs 
                        (event_id, game_id, season_id, home_player_id, away_player_id,
                         period, time_formatted, seconds, x_location, y_location, 
                         location_id, home_win, win_team_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        faceoff.get('id'),
                        int(faceoff['game_id']),
                        int(faceoff['season_id']),
                        int(faceoff['home_player_id']),
                        int(faceoff['visitor_player_id']),
                        int(faceoff['period']),
                        faceoff.get('time_formatted'),
                        int(faceoff['seconds']),
                        int(faceoff['x_location']) if faceoff.get('x_location') else None,
                        int(faceoff['y_location']) if faceoff.get('y_location') else None,
                        int(faceoff['location_id']) if faceoff.get('location_id') else None,
                        1 if faceoff.get('home_win') == '1' else 0,
                        int(faceoff['win_team_id'])
                    ))
                except Exception as e:
                    pass
            self.conn.commit()
            print(f"    ✓ {len(faceoffs)} faceoffs")
        
        # Hits
        print("  Fetching hits...")
        hits = self.fetch_csv(f"{GITHUB_RAW}/games/play_by_play/hits.csv")
        if hits:
            for hit in hits:
                try:
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO hits 
                        (event_id, game_id, season_id, player_id, team_id, is_home,
                         period, time_formatted, seconds, x_location, y_location, hit_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        hit.get('id'),
                        int(hit['game_id']),
                        int(hit['season_id']),
                        int(hit['player_id']),
                        int(hit['team_id']),
                        1 if hit.get('home') == '1' else 0,
                        int(hit['period']),
                        hit.get('time_formatted'),
                        int(hit['seconds']),
                        int(hit['x_location']) if hit.get('x_location') else None,
                        int(hit['y_location']) if hit.get('y_location') else None,
                        int(hit['hit_type']) if hit.get('hit_type') else None
                    ))
                except Exception as e:
                    pass
            self.conn.commit()
            print(f"    ✓ {len(hits)} hits")
        
        # Blocked shots
        print("  Fetching blocked shots...")
        blocked = self.fetch_csv(f"{GITHUB_RAW}/games/play_by_play/blocked_shots.csv")
        if blocked:
            for block in blocked:
                try:
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO blocked_shots 
                        (event_id, game_id, season_id, blocker_id, shooter_id, team_id,
                         opponent_team_id, is_home, period, time_formatted, seconds)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        block.get('id'),
                        int(block['game_id']),
                        int(block['season_id']),
                        int(block['blocker_player_id']) if block.get('blocker_player_id') else None,
                        int(block['player_id']) if block.get('player_id') else None,
                        int(block['blocker_team_id']),
                        int(block['team_id']),
                        1 if block.get('home') == '1' else 0,
                        int(block['period']),
                        block.get('time_formatted'),
                        int(block['seconds'])
                    ))
                except Exception as e:
                    pass
            self.conn.commit()
            print(f"    ✓ {len(blocked)} blocked shots")
    
    # =====================================================
    # MAIN SCRAPE METHOD
    # =====================================================
    
    def scrape_all(self):
        """Run complete scrape"""
        print("=" * 60)
        print("PWHL Database Scraper")
        print("=" * 60)
        print(f"Database: {self.db_path}")
        print(f"Seasons: {SEASONS}")
        
        self.init_database()
        self.scrape_seasons()
        self.scrape_teams()
        self.scrape_players()
        self.scrape_games()
        self.scrape_skater_stats()
        self.scrape_goalie_stats()
        self.scrape_team_stats()
        self.scrape_play_by_play()
        
        print("\n" + "=" * 60)
        print("Scrape Complete!")
        print("=" * 60)
        
        # Print summary
        tables = ['seasons', 'teams', 'players', 'roster_assignments', 'games',
                  'skater_stats', 'goalie_stats', 'team_stats', 'shots', 'goals',
                  'penalties', 'faceoffs', 'hits', 'blocked_shots']
        
        print("\nDatabase Summary:")
        for table in tables:
            count = self.cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} records")
    
    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    scraper = PWHLScraper()
    try:
        scraper.scrape_all()
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
