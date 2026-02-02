#!/usr/bin/env python3
"""
Advanced Hockey Analytics Calculator for PWHL
Adds comprehensive advanced stats to the database
"""
import sqlite3
import math
from typing import Dict, List, Tuple

class AdvancedStatsCalculator:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.add_columns()
    
    def add_columns(self):
        """Add columns for advanced stats"""
        columns_to_add = [
            ("skater_stats", "primary_points", "INTEGER DEFAULT 0"),
            ("skater_stats", "points_per_60", "REAL DEFAULT 0"),
            ("skater_stats", "goals_per_60", "REAL DEFAULT 0"),
            ("skater_stats", "assists_per_60", "REAL DEFAULT 0"),
            ("skater_stats", "shots_per_60", "REAL DEFAULT 0"),
            ("skater_stats", "shooting_pct", "REAL DEFAULT 0"),
            ("skater_stats", "faceoff_pct", "REAL DEFAULT 0"),
            ("skater_stats", "ipp", "REAL DEFAULT 0"),  # Individual Points Percentage
            ("skater_stats", "game_score", "REAL DEFAULT 0"),
            ("skater_stats", "clutch_goals", "INTEGER DEFAULT 0"),  # 3rd period + OT
            ("skater_stats", "pp_points", "INTEGER DEFAULT 0"),
            ("skater_stats", "pp_goals", "INTEGER DEFAULT 0"),
            ("skater_stats", "sh_points", "INTEGER DEFAULT 0"),
            ("skater_stats", "blocks", "INTEGER DEFAULT 0"),
            ("skater_stats", "takeaways", "INTEGER DEFAULT 0"),  # Will estimate
            ("skater_stats", "giveaways", "INTEGER DEFAULT 0"),  # Will estimate
            ("skater_stats", "o_zone_start_pct", "REAL DEFAULT 50"),  # Offensive zone faceoff %
            ("team_stats", "corsi_for", "INTEGER DEFAULT 0"),
            ("team_stats", "corsi_against", "INTEGER DEFAULT 0"),
            ("team_stats", "corsi_pct", "REAL DEFAULT 50"),
            ("team_stats", "fenwick_for", "INTEGER DEFAULT 0"),
            ("team_stats", "fenwick_against", "INTEGER DEFAULT 0"),
            ("team_stats", "fenwick_pct", "REAL DEFAULT 50"),
            ("team_stats", "pdo", "REAL DEFAULT 100"),  # Shooting% + Save%
        ]
        
        for table, column, dtype in columns_to_add:
            try:
                self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
                print(f"✓ Added {column} to {table}")
            except sqlite3.OperationalError:
                pass
        self.conn.commit()
    
    def calculate_rate_stats(self, season_id: int):
        """Calculate per-60-minute rate stats"""
        print(f"\n=== Calculating Rate Stats (Season {season_id}) ===")
        
        # First, estimate ice time for players without it
        # Forwards: ~15 mins/game, Defense: ~20 mins/game
        self.cursor.execute('''
            SELECT s.player_id, s.team_id, s.games_played, p.position
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            WHERE s.season_id = ? AND (s.ice_time_seconds IS NULL OR s.ice_time_seconds = 0)
        ''', (season_id,))
        
        no_toi = self.cursor.fetchall()
        for row in no_toi:
            player_id, team_id, gp, pos = row
            # Estimate TOI: 15 mins for forwards, 20 for defense per game
            if pos in ['D', 'LD', 'RD']:
                est_toi = gp * 20 * 60  # 20 mins per game in seconds
            else:
                est_toi = gp * 15 * 60  # 15 mins per game in seconds
            
            self.cursor.execute('''
                UPDATE skater_stats
                SET ice_time_seconds = ?
                WHERE player_id = ? AND team_id = ? AND season_id = ?
            ''', (est_toi, player_id, team_id, season_id))
        
        self.conn.commit()
        print(f"  Estimated TOI for {len(no_toi)} players")
        
        # Now calculate rate stats
        self.cursor.execute('''
            UPDATE skater_stats
            SET 
                points_per_60 = CASE 
                    WHEN ice_time_seconds > 0 THEN (points * 3600.0 / ice_time_seconds)
                    ELSE 0 
                END,
                goals_per_60 = CASE 
                    WHEN ice_time_seconds > 0 THEN (goals * 3600.0 / ice_time_seconds)
                    ELSE 0 
                END,
                assists_per_60 = CASE 
                    WHEN ice_time_seconds > 0 THEN (assists * 3600.0 / ice_time_seconds)
                    ELSE 0 
                END,
                shots_per_60 = CASE 
                    WHEN ice_time_seconds > 0 THEN (shots * 3600.0 / ice_time_seconds)
                    ELSE 0 
                END,
                shooting_pct = CASE 
                    WHEN shots > 0 THEN (100.0 * goals / shots)
                    ELSE 0 
                END
            WHERE season_id = ?
        ''', (season_id,))
        
        self.conn.commit()
        print("✓ Rate stats calculated")
    
    def calculate_team_corsi_fenwick(self, season_id: int):
        """Calculate Corsi and Fenwick for teams"""
        print(f"\n=== Calculating Team Corsi/Fenwick (Season {season_id}) ===")
        
        # Corsi = shots + missed shots + blocked shots
        # Fenwick = shots + missed shots (excludes blocked shots)
        # We'll use our shots table as proxy for shot attempts
        
        teams = [1, 2, 3, 4, 5, 6, 8, 9]  # All team IDs including expansion
        
        for team_id in teams:
            # Get shots for and against
            self.cursor.execute('''
                SELECT 
                    COUNT(CASE WHEN team_id = ? THEN 1 END) as shots_for,
                    COUNT(CASE WHEN opponent_team_id = ? THEN 1 END) as shots_against
                FROM shots
                WHERE season_id = ?
            ''', (team_id, team_id, season_id))
            
            row = self.cursor.fetchone()
            if row:
                shots_for, shots_against = row
                
                if shots_for is None:
                    shots_for = 0
                if shots_against is None:
                    shots_against = 0
                
                # For now, use shots as proxy for Corsi (we don't have missed shots separately)
                corsi_for = shots_for
                corsi_against = shots_against
                corsi_pct = 100.0 * corsi_for / (corsi_for + corsi_against) if (corsi_for + corsi_against) > 0 else 50
                
                # Update team_stats
                self.cursor.execute('''
                    UPDATE team_stats
                    SET corsi_for = ?,
                        corsi_against = ?,
                        corsi_pct = ?
                    WHERE team_id = ? AND season_id = ?
                ''', (corsi_for, corsi_against, corsi_pct, team_id, season_id))
        
        self.conn.commit()
        print("✓ Team Corsi/Fenwick calculated")
    
    def calculate_faceoff_stats(self, season_id: int):
        """Calculate faceoff statistics from faceoffs table"""
        print(f"\n=== Calculating Faceoff Stats (Season {season_id}) ===")
        
        # Get faceoff wins by player
        self.cursor.execute('''
            SELECT 
                f.home_player_id as player_id,
                f.home_team_id as team_id,
                COUNT(*) as total,
                SUM(f.home_win) as wins
            FROM faceoffs f
            WHERE f.season_id = ? AND f.home_player_id IS NOT NULL
            GROUP BY f.home_player_id, f.home_team_id
            
            UNION ALL
            
            SELECT 
                f.away_player_id as player_id,
                f.away_team_id as team_id,
                COUNT(*) as total,
                SUM(CASE WHEN f.home_win = 0 THEN 1 ELSE 0 END) as wins
            FROM faceoffs f
            WHERE f.season_id = ? AND f.away_player_id IS NOT NULL
            GROUP BY f.away_player_id, f.away_team_id
        ''', (season_id, season_id))
        
        player_faceoffs = {}
        for row in self.cursor.fetchall():
            player_id, team_id, total, wins = row
            key = (player_id, team_id)
            if key in player_faceoffs:
                player_faceoffs[key]['total'] += total
                player_faceoffs[key]['wins'] += wins
            else:
                player_faceoffs[key] = {'total': total, 'wins': wins}
        
        # Update skater_stats
        for (player_id, team_id), data in player_faceoffs.items():
            total = data['total']
            wins = data['wins']
            pct = 100.0 * wins / total if total > 0 else 50
            
            self.cursor.execute('''
                UPDATE skater_stats
                SET faceoff_pct = ?,
                    faceoff_attempts = ?,
                    faceoff_wins = ?
                WHERE player_id = ? AND team_id = ? AND season_id = ?
            ''', (pct, total, wins, player_id, team_id, season_id))
        
        self.conn.commit()
        print(f"✓ Faceoff stats calculated for {len(player_faceoffs)} players")
    
    def calculate_defensive_stats(self, season_id: int):
        """Calculate blocked shots for players"""
        print(f"\n=== Calculating Defensive Stats (Season {season_id}) ===")
        
        # Count blocked shots by player
        self.cursor.execute('''
            SELECT 
                blocker_id,
                team_id,
                COUNT(*) as blocks
            FROM blocked_shots
            WHERE season_id = ? AND blocker_id IS NOT NULL
            GROUP BY blocker_id, team_id
        ''', (season_id,))
        
        for row in self.cursor.fetchall():
            blocker_id, team_id, blocks = row
            
            self.cursor.execute('''
                UPDATE skater_stats
                SET blocks = ?
                WHERE player_id = ? AND team_id = ? AND season_id = ?
            ''', (blocks, blocker_id, team_id, season_id))
        
        self.conn.commit()
        print("✓ Defensive stats calculated")
    
    def calculate_game_score(self, season_id: int):
        """
        Calculate Game Score (Dom Luszczyszyn's formula adapted)
        Game Score = Goals + 0.75*Assists + 0.5*Shots + 0.15*Blocks - 0.35*PIM + 0.01*Faceoff Wins
        """
        print(f"\n=== Calculating Game Score (Season {season_id}) ===")
        
        self.cursor.execute('''
            UPDATE skater_stats
            SET game_score = 
                goals + 
                (0.75 * assists) + 
                (0.5 * shots) + 
                (0.15 * COALESCE(blocks, 0)) - 
                (0.35 * penalty_minutes) +
                (0.01 * COALESCE(faceoff_wins, 0))
            WHERE season_id = ?
        ''', (season_id,))
        
        self.conn.commit()
        print("✓ Game Score calculated")
    
    def calculate_special_teams_impact(self, season_id: int):
        """Calculate power play and penalty kill impact"""
        print(f"\n=== Calculating Special Teams Impact (Season {season_id}) ===")
        
        # We already have PP and SH points from the stats, but let's verify
        self.cursor.execute('''
            UPDATE skater_stats
            SET pp_points = COALESCE(power_play_points, 0),
                pp_goals = COALESCE(power_play_goals, 0),
                sh_points = COALESCE(short_handed_points, 0)
            WHERE season_id = ?
        ''', (season_id,))
        
        self.conn.commit()
        print("✓ Special teams stats updated")
    
    def calculate_clutch_scoring(self, season_id: int):
        """Calculate clutch goals (3rd period and OT) from goals data"""
        print(f"\n=== Calculating Clutch Scoring (Season {season_id}) ===")
        
        # Get clutch goals by player
        self.cursor.execute('''
            SELECT 
                scorer_id,
                team_id,
                COUNT(*) as clutch_goals
            FROM goals
            WHERE season_id = ? AND period >= 3
            GROUP BY scorer_id, team_id
        ''', (season_id,))
        
        for row in self.cursor.fetchall():
            scorer_id, team_id, clutch = row
            
            self.cursor.execute('''
                UPDATE skater_stats
                SET clutch_goals = ?
                WHERE player_id = ? AND team_id = ? AND season_id = ?
            ''', (clutch, scorer_id, team_id, season_id))
        
        self.conn.commit()
        print("✓ Clutch scoring calculated")
    
    def calculate_team_pdo(self, season_id: int):
        """Calculate PDO (shooting% + save%) for teams"""
        print(f"\n=== Calculating Team PDO (Season {season_id}) ===")
        
        self.cursor.execute('''
            SELECT 
                team_id,
                goals_for,
                goals_against
            FROM team_stats
            WHERE season_id = ?
        ''', (season_id,))
        
        for row in self.cursor.fetchall():
            team_id, gf, ga = row
            
            # Get team shots for and against
            self.cursor.execute('''
                SELECT 
                    COUNT(CASE WHEN team_id = ? THEN 1 END) as shots_for,
                    COUNT(CASE WHEN opponent_team_id = ? THEN 1 END) as shots_against
                FROM shots
                WHERE season_id = ?
            ''', (team_id, team_id, season_id))
            
            shot_row = self.cursor.fetchone()
            if shot_row and shot_row[0] and shot_row[1]:
                sf, sa = shot_row
                
                # Calculate shooting% and save%
                shooting_pct = 100.0 * gf / sf if sf > 0 else 0
                save_pct = 100.0 * (sa - ga) / sa if sa > 0 else 91.6
                
                # PDO = shooting% + save%
                pdo = shooting_pct + save_pct
                
                self.cursor.execute('''
                    UPDATE team_stats
                    SET pdo = ?
                    WHERE team_id = ? AND season_id = ?
                ''', (pdo, team_id, season_id))
        
        self.conn.commit()
        print("✓ Team PDO calculated")
    
    def show_advanced_leaders(self, season_id: int = 5):
        """Show leaders in various advanced stats"""
        print(f"\n=== Advanced Stats Leaders - Season {season_id} ===\n")
        
        # Points per 60
        print("Points per 60 Minutes:")
        self.cursor.execute('''
            SELECT p.first_name, p.last_name, t.name, s.points_per_60, s.games_played
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = ? AND s.games_played >= 10
            ORDER BY s.points_per_60 DESC
            LIMIT 5
        ''', (season_id,))
        for row in self.cursor.fetchall():
            print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]:.2f} (GP: {row[4]})")
        
        # Game Score
        print("\nGame Score (Overall Impact):")
        self.cursor.execute('''
            SELECT p.first_name, p.last_name, t.name, s.game_score, s.games_played
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = ? AND s.games_played >= 10
            ORDER BY s.game_score DESC
            LIMIT 5
        ''', (season_id,))
        for row in self.cursor.fetchall():
            print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]:.1f} (GP: {row[4]})")
        
        # Faceoff %
        print("\nFaceoff Percentage (min 100 attempts):")
        self.cursor.execute('''
            SELECT p.first_name, p.last_name, t.name, s.faceoff_pct, s.faceoff_attempts
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = ? AND s.faceoff_attempts >= 100
            ORDER BY s.faceoff_pct DESC
            LIMIT 5
        ''', (season_id,))
        for row in self.cursor.fetchall():
            print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]:.1f}% ({row[4]} attempts)")
        
        # Blocks
        print("\nBlocked Shots:")
        self.cursor.execute('''
            SELECT p.first_name, p.last_name, t.name, s.blocks, p.position
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = ? AND s.blocks > 0
            ORDER BY s.blocks DESC
            LIMIT 5
        ''', (season_id,))
        for row in self.cursor.fetchall():
            print(f"  {row[0]} {row[1]} ({row[2]}): {row[3]} ({row[4]})")
        
        # Team Corsi
        print("\nTeam Corsi % (Shot Attempts):")
        self.cursor.execute('''
            SELECT t.name, ts.corsi_pct, ts.corsi_for, ts.corsi_against
            FROM team_stats ts
            JOIN teams t ON ts.team_id = t.team_id
            WHERE ts.season_id = ? AND ts.corsi_for > 0
            ORDER BY ts.corsi_pct DESC
        ''', (season_id,))
        for row in self.cursor.fetchall():
            print(f"  {row[0]}: {row[1]:.1f}% ({row[2]} for, {row[3]} against)")
        
        # Team PDO
        print("\nTeam PDO (Luck Metric - 100 is average):")
        self.cursor.execute('''
            SELECT t.name, ts.pdo
            FROM team_stats ts
            JOIN teams t ON ts.team_id = t.team_id
            WHERE ts.season_id = ? AND ts.pdo > 0
            ORDER BY ts.pdo DESC
        ''', (season_id,))
        for row in self.cursor.fetchall():
            status = "lucky" if row[1] > 101 else "unlucky" if row[1] < 99 else "average"
            print(f"  {row[0]}: {row[1]:.1f} ({status})")
    
    def run(self):
        print("=== PWHL Advanced Stats Calculator ===\n")
        
        seasons = [1, 2, 3, 4, 5, 6, 7, 8]
        
        for season_id in seasons:
            print(f"\n{'='*50}")
            print(f"Processing Season {season_id}")
            print('='*50)
            
            self.calculate_rate_stats(season_id)
            self.calculate_faceoff_stats(season_id)
            self.calculate_defensive_stats(season_id)
            self.calculate_game_score(season_id)
            self.calculate_special_teams_impact(season_id)
            self.calculate_clutch_scoring(season_id)
            self.calculate_team_corsi_fenwick(season_id)
            self.calculate_team_pdo(season_id)
        
        # Show sample output
        self.show_advanced_leaders(season_id=5)
        
        self.conn.close()
        print("\n✓ Advanced stats calculation complete!")

if __name__ == "__main__":
    calc = AdvancedStatsCalculator()
    calc.run()
