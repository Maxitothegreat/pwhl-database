#!/usr/bin/env python3
"""
Calculate Expected Goals (xG) for PWHL skaters using shot location data
Calibrated based on actual shooting percentages from the data
"""
import sqlite3
import math
from typing import Dict, Tuple

def calculate_distance_from_offensive_zone(y: int) -> float:
    """
    Calculate distance from offensive zone
    Center ice is at y=147 (294/2)
    Offensive zones are y < 100 (attacking top goal) or y > 200 (attacking bottom goal)
    """
    center_y = 147
    return abs(y - center_y)

def calculate_xg(shot_type: int, quality: int, x: int, y: int, is_goal: int) -> float:
    """
    Calculate xG based on actual shooting percentages from PWHL data
    
    Shot distribution analysis:
    - y < 100 or y > 200: 4% shooting (perimeter/defensive)
    - y 100-200: 12% shooting (offensive zone)
    
    Shot type shooting %:
    - Tip: 13.38%
    - Snap: 10.33%
    - Default: 9.19%
    - Backhand: 9.18%
    - Slap: 8.73%
    - Wrist: 7.36%
    """
    # Base xG by zone
    if y < 100 or y > 200:
        # Perimeter/defensive zone - 4% base
        base_xg = 0.04
    else:
        # Offensive zone - 12% base
        base_xg = 0.12
    
    # Shot type adjustment (relative to wrist shot baseline of 7.36%)
    type_multipliers = {
        1: 1.40,  # Snap: 10.33/7.36 = 1.40
        2: 1.00,  # Wrist: 7.36/7.36 = 1.00 (baseline)
        3: 1.19,  # Slap: 8.73/7.36 = 1.19
        4: 1.25,  # Backhand: 9.18/7.36 = 1.25
        5: 1.25,  # Default: 9.19/7.36 = 1.25
        6: 1.82   # Tip: 13.38/7.36 = 1.82
    }
    type_mult = type_multipliers.get(shot_type, 1.0)
    
    # Quality adjustment
    # Quality 1,5 = Quality on net/goal (higher danger)
    # Quality 2,6 = Non quality on net/goal (lower danger)
    quality_multipliers = {
        1: 1.15,  # Quality on net
        2: 0.85,  # Non quality on net
        5: 1.15,  # Quality goal
        6: 0.85   # Non quality goal
    }
    quality_mult = quality_multipliers.get(quality, 1.0)
    
    # Calculate xG
    xg = base_xg * type_mult * quality_mult
    
    # Cap at 0.25 (25% chance - very high danger)
    return min(xg, 0.25)

class xGCalculator:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def add_xg_column(self):
        """Add xG column to skater_stats table"""
        try:
            self.cursor.execute("ALTER TABLE skater_stats ADD COLUMN xg REAL DEFAULT 0")
            print("✓ Added xG column to skater_stats")
        except sqlite3.OperationalError:
            pass
        try:
            self.cursor.execute("ALTER TABLE skater_stats ADD COLUMN goals_above_xg REAL DEFAULT 0")
            print("✓ Added goals_above_xg column to skater_stats")
        except sqlite3.OperationalError:
            pass
        self.conn.commit()
    
    def calculate_shot_xg(self):
        """Calculate xG for each shot"""
        print("\n=== Calculating xG for all shots ===")
        
        # Add xg column to shots if not exists
        try:
            self.cursor.execute("ALTER TABLE shots ADD COLUMN xg REAL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        
        # Get all shots
        self.cursor.execute('''
            SELECT id, x_location, y_location, shot_type, quality, is_goal
            FROM shots
        ''')
        shots = self.cursor.fetchall()
        
        print(f"Processing {len(shots)} shots...")
        
        updates = []
        for shot in shots:
            shot_id, x, y, shot_type, quality, is_goal = shot
            xg = calculate_xg(shot_type, quality, x, y, is_goal)
            updates.append((xg, shot_id))
        
        # Batch update
        self.cursor.executemany('UPDATE shots SET xg = ? WHERE id = ?', updates)
        self.conn.commit()
        
        print(f"✓ Calculated xG for {len(updates)} shots")
        
        # Show distribution
        self.cursor.execute('SELECT AVG(xg), MAX(xg), SUM(xg), SUM(is_goal) FROM shots')
        avg_xg, max_xg, total_xg, total_goals = self.cursor.fetchone()
        print(f"  Average xG per shot: {avg_xg:.3f}")
        print(f"  Max xG: {max_xg:.3f}")
        print(f"  Total xG: {total_xg:.1f}")
        print(f"  Actual goals: {int(total_goals)}")
        print(f"  Difference: {int(total_goals) - total_xg:.1f}")
    
    def aggregate_xg_by_player(self):
        """Aggregate xG by player and season"""
        print("\n=== Aggregating xG by player ===")
        
        # Clear existing xG data
        self.cursor.execute("UPDATE skater_stats SET xg = 0, goals_above_xg = 0")
        self.conn.commit()
        
        # Aggregate xG from shots
        self.cursor.execute('''
            SELECT 
                player_id,
                team_id,
                season_id,
                SUM(xg) as total_xg,
                SUM(is_goal) as actual_goals,
                COUNT(*) as shots
            FROM shots
            GROUP BY player_id, team_id, season_id
        ''')
        
        player_xg = self.cursor.fetchall()
        print(f"Found {len(player_xg)} player/season/team combinations with shots")
        
        # Update skater_stats
        updated = 0
        for row in player_xg:
            player_id, team_id, season_id, total_xg, actual_goals, shots = row
            goals_above_xg = actual_goals - total_xg
            
            self.cursor.execute('''
                UPDATE skater_stats 
                SET xg = ?, goals_above_xg = ?
                WHERE player_id = ? AND team_id = ? AND season_id = ?
            ''', (total_xg, goals_above_xg, player_id, team_id, season_id))
            
            if self.cursor.rowcount > 0:
                updated += 1
        
        self.conn.commit()
        print(f"✓ Updated xG for {updated} skater stat records")
    
    def show_xg_leaders(self, season_id: int = 5, limit: int = 15):
        """Show xG leaders for a season"""
        self.cursor.execute('''
            SELECT season_name FROM seasons WHERE season_id = ?
        ''', (season_id,))
        season_name = self.cursor.fetchone()[0]
        
        print(f"\n=== {season_name} xG Leaders ===")
        
        self.cursor.execute('''
            SELECT 
                p.first_name,
                p.last_name,
                t.name as team,
                s.goals,
                ROUND(s.xg, 1) as xg,
                ROUND(s.goals_above_xg, 1) as gax,
                s.shots
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = ? AND s.xg > 0
            ORDER BY s.xg DESC
            LIMIT ?
        ''', (season_id, limit))
        
        print(f"{'Rank':<5} {'Player':<25} {'Team':<18} {'G':<4} {'xG':<6} {'G-xG':<6} {'Shots':<6}")
        print("-" * 75)
        rank = 1
        for row in self.cursor.fetchall():
            name = f"{row[0]} {row[1]}"
            print(f"{rank:<5} {name:<25} {row[2]:<18} {row[3]:<4} {row[4]:<6} {row[5]:<+6} {row[6]:<6}")
            rank += 1
        
        # Show goals above xG leaders (finishing skill)
        print(f"\n=== {season_name} Goals Above xG Leaders (Finishing Skill) ===")
        self.cursor.execute('''
            SELECT 
                p.first_name,
                p.last_name,
                t.name as team,
                s.goals,
                ROUND(s.xg, 1) as xg,
                ROUND(s.goals_above_xg, 1) as gax,
                s.shots
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            JOIN teams t ON s.team_id = t.team_id
            WHERE s.season_id = ? AND s.xg > 3
            ORDER BY s.goals_above_xg DESC
            LIMIT 10
        ''', (season_id,))
        
        print(f"{'Rank':<5} {'Player':<25} {'Team':<18} {'G':<4} {'xG':<6} {'G-xG':<6} {'Shots':<6}")
        print("-" * 75)
        rank = 1
        for row in self.cursor.fetchall():
            name = f"{row[0]} {row[1]}"
            print(f"{rank:<5} {name:<25} {row[2]:<18} {row[3]:<4} {row[4]:<6} {row[5]:<+6} {row[6]:<6}")
            rank += 1
    
    def run(self):
        print("=== PWHL Expected Goals (xG) Calculator ===\n")
        
        # Add xG columns
        self.add_xg_column()
        
        # Calculate xG for each shot
        self.calculate_shot_xg()
        
        # Aggregate by player
        self.aggregate_xg_by_player()
        
        # Show leaders for all seasons
        self.show_xg_leaders(season_id=5, limit=15)
        self.show_xg_leaders(season_id=1, limit=10)
        self.show_xg_leaders(season_id=8, limit=10)
        
        self.conn.close()
        print("\n✓ xG calculation complete!")

if __name__ == "__main__":
    calc = xGCalculator()
    calc.run()
