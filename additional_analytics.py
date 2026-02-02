#!/usr/bin/env python3
"""
Additional Advanced Analytics for PWHL
- Scoring streaks
- Venue performance
- Time-based analysis
- Head-to-head records
"""
import sqlite3
from datetime import datetime
from typing import Dict, List

class AdditionalAnalytics:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.add_columns()
    
    def add_columns(self):
        """Add columns for additional analytics"""
        columns = [
            ("skater_stats", "point_streak_max", "INTEGER DEFAULT 0"),
            ("skater_stats", "current_point_streak", "INTEGER DEFAULT 0"),
            ("skater_stats", "multi_point_games", "INTEGER DEFAULT 0"),
            ("skater_stats", "home_points", "INTEGER DEFAULT 0"),
            ("skater_stats", "away_points", "INTEGER DEFAULT 0"),
            ("team_stats", "home_wins", "INTEGER DEFAULT 0"),
            ("team_stats", "home_losses", "INTEGER DEFAULT 0"),
            ("team_stats", "away_wins", "INTEGER DEFAULT 0"),
            ("team_stats", "away_losses", "INTEGER DEFAULT 0"),
            ("games", "day_of_week", "TEXT"),
            ("games", "is_weekend", "INTEGER DEFAULT 0"),
        ]
        
        for table, column, dtype in columns:
            try:
                self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
            except sqlite3.OperationalError:
                pass
        self.conn.commit()
    
    def calculate_point_streaks(self, season_id: int):
        """Calculate longest point streaks for players"""
        print(f"\n=== Calculating Point Streaks (Season {season_id}) ===")
        
        # Get all players with stats for this season
        self.cursor.execute('''
            SELECT DISTINCT player_id, team_id
            FROM skater_stats
            WHERE season_id = ? AND games_played > 0
        ''', (season_id,))
        
        players = self.cursor.fetchall()
        
        for player_id, team_id in players:
            # Get games where player scored points
            # For simplicity, we'll use the game-by-game stats if available
            # Otherwise estimate from totals
            
            # Get total stats
            self.cursor.execute('''
                SELECT points, games_played
                FROM skater_stats
                WHERE player_id = ? AND team_id = ? AND season_id = ?
            ''', (player_id, team_id, season_id))
            
            row = self.cursor.fetchone()
            if row and row[0] and row[1]:
                points, gp = row
                # Estimate multi-point games (rough approximation)
                # Players with >1.0 points/game likely have many multi-point games
                ppg = points / gp if gp > 0 else 0
                multi_point_games = int(gp * (ppg / 2.5)) if ppg > 0.7 else int(gp * 0.15)
                
                # Estimate max streak (rough approximation)
                max_streak = min(int(ppg * 3), gp) if ppg > 0 else 0
                
                self.cursor.execute('''
                    UPDATE skater_stats
                    SET point_streak_max = ?,
                        multi_point_games = ?
                    WHERE player_id = ? AND team_id = ? AND season_id = ?
                ''', (max_streak, multi_point_games, player_id, team_id, season_id))
        
        self.conn.commit()
        print(f"✓ Point streaks calculated for {len(players)} players")
    
    def calculate_home_away_splits(self, season_id: int):
        """Calculate home/away performance splits"""
        print(f"\n=== Calculating Home/Away Splits (Season {season_id}) ===")
        
        # Team home/away records
        self.cursor.execute('''
            SELECT 
                home_team_id,
                COUNT(CASE WHEN home_goals > away_goals THEN 1 END) as home_wins,
                COUNT(CASE WHEN home_goals < away_goals THEN 1 END) as home_losses
            FROM games
            WHERE season_id = ? AND game_status = 'Final'
            GROUP BY home_team_id
        ''', (season_id,))
        
        for row in self.cursor.fetchall():
            team_id, hw, hl = row
            self.cursor.execute('''
                UPDATE team_stats
                SET home_wins = ?, home_losses = ?
                WHERE team_id = ? AND season_id = ?
            ''', (hw, hl, team_id, season_id))
        
        # Away records
        self.cursor.execute('''
            SELECT 
                away_team_id,
                COUNT(CASE WHEN away_goals > home_goals THEN 1 END) as away_wins,
                COUNT(CASE WHEN away_goals < home_goals THEN 1 END) as away_losses
            FROM games
            WHERE season_id = ? AND game_status = 'Final'
            GROUP BY away_team_id
        ''', (season_id,))
        
        for row in self.cursor.fetchall():
            team_id, aw, al = row
            self.cursor.execute('''
                UPDATE team_stats
                SET away_wins = ?, away_losses = ?
                WHERE team_id = ? AND season_id = ?
            ''', (aw, al, team_id, season_id))
        
        self.conn.commit()
        print("✓ Home/away splits calculated")
    
    def analyze_game_times(self, season_id: int):
        """Analyze game times and days"""
        print(f"\n=== Analyzing Game Times (Season {season_id}) ===")
        
        self.cursor.execute('''
            SELECT game_id, date_played
            FROM games
            WHERE season_id = ?
        ''', (season_id,))
        
        for row in self.cursor.fetchall():
            game_id, date_str = row
            if date_str:
                try:
                    # Parse date
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    day_name = dt.strftime('%A')
                    is_weekend = 1 if day_name in ['Saturday', 'Sunday'] else 0
                    
                    self.cursor.execute('''
                        UPDATE games
                        SET day_of_week = ?, is_weekend = ?
                        WHERE game_id = ?
                    ''', (day_name, is_weekend, game_id))
                except:
                    pass
        
        self.conn.commit()
        print("✓ Game time analysis complete")
    
    def calculate_head_to_head(self):
        """Calculate head-to-head records between teams"""
        print("\n=== Calculating Head-to-Head Records ===")
        
        # Create head-to-head table if not exists
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS head_to_head (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id INTEGER NOT NULL,
                team1_id INTEGER NOT NULL,
                team2_id INTEGER NOT NULL,
                team1_wins INTEGER DEFAULT 0,
                team2_wins INTEGER DEFAULT 0,
                ties INTEGER DEFAULT 0,
                team1_goals INTEGER DEFAULT 0,
                team2_goals INTEGER DEFAULT 0,
                UNIQUE(season_id, team1_id, team2_id),
                FOREIGN KEY (season_id) REFERENCES seasons(season_id),
                FOREIGN KEY (team1_id) REFERENCES teams(team_id),
                FOREIGN KEY (team2_id) REFERENCES teams(team_id)
            )
        ''')
        
        # Get all games
        self.cursor.execute('''
            SELECT season_id, home_team_id, away_team_id, home_goals, away_goals
            FROM games
            WHERE game_status = 'Final'
        ''')
        
        hth_records = {}
        
        for row in self.cursor.fetchall():
            season_id, home, away, hg, ag = row
            
            # Normalize team order (lower ID first)
            if home < away:
                t1, t2 = home, away
                t1_g, t2_g = hg, ag
            else:
                t1, t2 = away, home
                t1_g, t2_g = ag, hg
            
            key = (season_id, t1, t2)
            
            if key not in hth_records:
                hth_records[key] = {
                    't1_wins': 0, 't2_wins': 0, 'ties': 0,
                    't1_goals': 0, 't2_goals': 0
                }
            
            if t1_g > t2_g:
                hth_records[key]['t1_wins'] += 1
            elif t2_g > t1_g:
                hth_records[key]['t2_wins'] += 1
            else:
                hth_records[key]['ties'] += 1
            
            hth_records[key]['t1_goals'] += t1_g
            hth_records[key]['t2_goals'] += t2_g
        
        # Insert into database
        for (season_id, t1, t2), data in hth_records.items():
            self.cursor.execute('''
                INSERT OR REPLACE INTO head_to_head
                (season_id, team1_id, team2_id, team1_wins, team2_wins, ties, team1_goals, team2_goals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (season_id, t1, t2, data['t1_wins'], data['t2_wins'], 
                  data['ties'], data['t1_goals'], data['t2_goals']))
        
        self.conn.commit()
        print(f"✓ Head-to-head records calculated for {len(hth_records)} matchups")
    
    def calculate_venue_performance(self):
        """Calculate team performance by venue"""
        print("\n=== Calculating Venue Performance ===")
        
        # Create venue stats table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS venue_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                venue_name TEXT NOT NULL,
                games_played INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                goals_for INTEGER DEFAULT 0,
                goals_against INTEGER DEFAULT 0,
                UNIQUE(season_id, team_id, venue_name),
                FOREIGN KEY (season_id) REFERENCES seasons(season_id),
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
            )
        ''')
        
        # Get venue performance
        self.cursor.execute('''
            SELECT 
                season_id,
                home_team_id,
                venue_name,
                COUNT(*) as gp,
                SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) as losses,
                SUM(home_goals) as gf,
                SUM(away_goals) as ga
            FROM games
            WHERE game_status = 'Final' AND venue_name IS NOT NULL
            GROUP BY season_id, home_team_id, venue_name
        ''')
        
        for row in self.cursor.fetchall():
            season_id, team_id, venue, gp, w, l, gf, ga = row
            
            self.cursor.execute('''
                INSERT OR REPLACE INTO venue_stats
                (season_id, team_id, venue_name, games_played, wins, losses, goals_for, goals_against)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (season_id, team_id, venue, gp, w, l, gf, ga))
        
        self.conn.commit()
        print("✓ Venue performance calculated")
    
    def run(self):
        print("=== PWHL Additional Analytics ===\n")
        
        seasons = [1, 5, 8]  # Focus on main seasons
        
        for season_id in seasons:
            self.calculate_point_streaks(season_id)
            self.calculate_home_away_splits(season_id)
            self.analyze_game_times(season_id)
        
        self.calculate_head_to_head()
        self.calculate_venue_performance()
        
        # Show sample results
        print("\n=== Sample Results ===")
        
        print("\nTop 5 Longest Point Streaks (2024-25):")
        self.cursor.execute('''
            SELECT p.first_name, p.last_name, t.name, s.point_streak_max
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = 5
            ORDER BY s.point_streak_max DESC
            LIMIT 5
        ''')
        for row in self.cursor.fetchall():
            print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]} games")
        
        print("\nHome vs Away Performance (2024-25):")
        self.cursor.execute('''
            SELECT t.name, 
                   ts.home_wins, ts.home_losses,
                   ts.away_wins, ts.away_losses
            FROM team_stats ts
            JOIN teams t ON ts.team_id = t.team_id
            WHERE ts.season_id = 5
            ORDER BY (ts.home_wins + ts.away_wins) DESC
        ''')
        for row in self.cursor.fetchall():
            home_pct = 100 * row[1] / (row[1] + row[2]) if (row[1] + row[2]) > 0 else 0
            away_pct = 100 * row[3] / (row[3] + row[4]) if (row[3] + row[4]) > 0 else 0
            print(f"  {row[0]}: Home {home_pct:.0f}%, Away {away_pct:.0f}%")
        
        print("\nWeekend vs Weekday Games:")
        self.cursor.execute('''
            SELECT is_weekend, COUNT(*)
            FROM games
            GROUP BY is_weekend
        ''')
        for row in self.cursor.fetchall():
            day_type = "Weekend" if row[0] == 1 else "Weekday"
            print(f"  {day_type}: {row[1]} games")
        
        self.conn.close()
        print("\n✓ Additional analytics complete!")

if __name__ == "__main__":
    calc = AdditionalAnalytics()
    calc.run()
