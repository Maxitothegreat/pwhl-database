#!/usr/bin/env python3
"""
Estimate xG for 2025-26 season (Season 8) using available player stats
Since shot-level play-by-play is not available, we estimate based on:
- Shot volume
- Shooting percentage
- Historical conversion rates by position/shot type
"""
import sqlite3
import math

class xGEstimator:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Load historical conversion rates from seasons 1 & 5
        self.load_historical_rates()
    
    def load_historical_rates(self):
        """Calculate historical shooting percentages by position"""
        print("=== Loading Historical Conversion Rates ===\n")
        
        # Get shooting % by position from seasons with full data
        self.cursor.execute('''
            SELECT 
                p.position,
                SUM(s.shots) as total_shots,
                SUM(s.goals) as total_goals,
                AVG(1.0 * s.goals / NULLIF(s.shots, 0)) as avg_shooting_pct
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            WHERE s.season_id IN (1, 5) AND s.shots > 0
            GROUP BY p.position
        ''')
        
        self.position_rates = {}
        for row in self.cursor.fetchall():
            position, shots, goals, avg_pct = row
            self.position_rates[position] = {
                'shots': shots,
                'goals': goals,
                'shooting_pct': avg_pct if avg_pct else 0.10
            }
            print(f"  {position}: {goals}/{shots} = {avg_pct:.3f} ({avg_pct*100:.1f}%)")
        
        # Default rate
        self.default_rate = 0.10  # 10% baseline
        
        # Get league average shooting % by shot volume buckets
        self.cursor.execute('''
            SELECT 
                CASE 
                    WHEN shots < 20 THEN 'low'
                    WHEN shots < 50 THEN 'medium'
                    WHEN shots < 80 THEN 'high'
                    ELSE 'very_high'
                END as volume_bucket,
                AVG(1.0 * goals / NULLIF(shots, 0)) as avg_conversion
            FROM skater_stats
            WHERE season_id IN (1, 5) AND shots > 0
            GROUP BY volume_bucket
        ''')
        
        self.volume_rates = {}
        for row in self.cursor.fetchall():
            bucket, rate = row
            self.volume_rates[bucket] = rate if rate else self.default_rate
            print(f"  {bucket} volume: {rate:.3f} conversion")
    
    def estimate_xg_for_player(self, shots: int, goals: int, position: str) -> float:
        """
        Estimate xG based on:
        - Shot volume
        - Actual goals scored
        - Position-based expected conversion rate
        """
        if shots == 0:
            return 0.0
        
        # Get position-based expected shooting %
        position_rate = self.position_rates.get(position, {}).get('shooting_pct', self.default_rate)
        
        # Calculate volume bucket
        if shots < 20:
            bucket = 'low'
        elif shots < 50:
            bucket = 'medium'
        elif shots < 80:
            bucket = 'high'
        else:
            bucket = 'very_high'
        
        volume_rate = self.volume_rates.get(bucket, self.default_rate)
        
        # Weighted average: position rate (60%) + volume rate (40%)
        expected_rate = (position_rate * 0.6) + (volume_rate * 0.4)
        
        # Calculate base xG
        base_xg = shots * expected_rate
        
        # Adjust for actual performance (regression to mean)
        # If player scored way above/below expected, adjust slightly
        actual_rate = goals / shots if shots > 0 else 0
        
        # Regression factor: more shots = trust actual performance more
        if shots < 20:
            regression = 0.8  # Trust expected more
        elif shots < 50:
            regression = 0.6
        else:
            regression = 0.4  # Trust actual more
        
        # Final xG estimate
        estimated_xg = (base_xg * regression) + (goals * (1 - regression))
        
        return max(0, estimated_xg)  # No negative xG
    
    def estimate_season_8_xg(self):
        """Estimate xG for all season 8 players"""
        print("\n=== Estimating xG for 2025-26 Season ===\n")
        
        # Get all season 8 players with stats
        self.cursor.execute('''
            SELECT 
                s.player_id,
                s.team_id,
                s.season_id,
                s.shots,
                s.goals,
                p.position
            FROM skater_stats s
            JOIN players p ON s.player_id = p.player_id
            WHERE s.season_id = 8 AND s.shots > 0
        ''')
        
        players = self.cursor.fetchall()
        print(f"Found {len(players)} players with shots in season 8")
        
        updates = []
        for row in players:
            player_id, team_id, season_id, shots, goals, position = row
            
            # Estimate xG
            xg = self.estimate_xg_for_player(shots, goals, position)
            goals_above_xg = goals - xg
            
            updates.append((xg, goals_above_xg, player_id, team_id, season_id))
        
        # Update database
        self.cursor.executemany('''
            UPDATE skater_stats 
            SET xg = ?, goals_above_xg = ?
            WHERE player_id = ? AND team_id = ? AND season_id = ?
        ''', updates)
        
        self.conn.commit()
        print(f"✓ Updated xG for {len(updates)} players")
    
    def show_estimated_leaders(self, limit: int = 15):
        """Show estimated xG leaders for season 8"""
        print(f"\n=== 2025-26 Estimated xG Leaders ===")
        
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
            WHERE s.season_id = 8 AND s.xg > 0
            ORDER BY s.xg DESC
            LIMIT ?
        ''', (limit,))
        
        print(f"{'Rank':<5} {'Player':<25} {'Team':<18} {'G':<4} {'xG*':<6} {'G-xG':<6} {'Shots':<6}")
        print("-" * 75)
        print("* Estimated (no shot-level data available)")
        print()
        
        rank = 1
        for row in self.cursor.fetchall():
            name = f"{row[0]} {row[1]}"
            print(f"{rank:<5} {name:<25} {row[2]:<18} {row[3]:<4} {row[4]:<6} {row[5]:<+6} {row[6]:<6}")
            rank += 1
        
        # Show over/underperformers
        print(f"\n=== Goals Above Estimated xG (Finishing Skill) ===")
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
            WHERE s.season_id = 8 AND s.xg > 2
            ORDER BY s.goals_above_xg DESC
            LIMIT 10
        ''')
        
        print(f"{'Rank':<5} {'Player':<25} {'Team':<18} {'G':<4} {'xG*':<6} {'G-xG':<6} {'Shots':<6}")
        print("-" * 75)
        rank = 1
        for row in self.cursor.fetchall():
            name = f"{row[0]} {row[1]}"
            print(f"{rank:<5} {name:<25} {row[2]:<18} {row[3]:<4} {row[4]:<6} {row[5]:<+6} {row[6]:<6}")
            rank += 1
    
    def run(self):
        print("=== PWHL xG Estimator for 2025-26 Season ===\n")
        
        # Estimate xG for season 8
        self.estimate_season_8_xg()
        
        # Show leaders
        self.show_estimated_leaders()
        
        self.conn.close()
        print("\n✓ xG estimation complete!")
        print("\nNote: 2025-26 xG values are ESTIMATED based on shot volume and")
        print("historical conversion rates. Actual shot-level data is not available.")

if __name__ == "__main__":
    estimator = xGEstimator()
    estimator.run()
