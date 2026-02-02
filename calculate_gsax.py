#!/usr/bin/env python3
"""
Calculate GSAx (Goals Saved Above Expected) for PWHL goalies
"""
import sqlite3

class GSAxCalculator:
    def __init__(self, db_path: str = "pwhl_database.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Add GSAx column if not exists
        self.add_gsax_column()
    
    def add_gsax_column(self):
        """Add GSAx column to goalie_stats table"""
        try:
            self.cursor.execute("ALTER TABLE goalie_stats ADD COLUMN gsax REAL DEFAULT 0")
            print("✓ Added GSAx column to goalie_stats")
        except sqlite3.OperationalError:
            print("  GSAx column already exists")
        self.conn.commit()
    
    def calculate_gsax_from_shots(self, season_id: int):
        """
        Calculate GSAx for goalies using shot-level xG data
        GSAx = Expected Goals Against - Actual Goals Against
        Positive = Better than average (saved more goals than expected)
        
        Note: In shots table, opponent_team_id is the goalie's team
        """
        print(f"\n=== Calculating GSAx from Shot Data (Season {season_id}) ===")
        
        # Get xG and goals for each goalie
        # opponent_team_id is the team being shot against (the goalie's team)
        self.cursor.execute('''
            SELECT 
                goalie_id,
                opponent_team_id,
                season_id,
                SUM(xg) as expected_goals,
                SUM(is_goal) as actual_goals,
                COUNT(*) as shots_faced
            FROM shots
            WHERE goalie_id IS NOT NULL AND season_id = ?
            GROUP BY goalie_id, opponent_team_id
        ''', (season_id,))
        
        goalie_data = self.cursor.fetchall()
        print(f"Found {len(goalie_data)} goalie/team combinations with shot data")
        
        updates = []
        for row in goalie_data:
            goalie_id, team_id, season_id, xg_against, goals_against, shots = row
            
            # GSAx = Expected Goals - Actual Goals
            # Positive = saved more than expected (good)
            # Negative = allowed more than expected (bad)
            gsax = xg_against - goals_against
            
            updates.append((gsax, goalie_id, team_id, season_id))
        
        # Update goalie_stats
        self.cursor.executemany('''
            UPDATE goalie_stats 
            SET gsax = ?
            WHERE player_id = ? AND team_id = ? AND season_id = ?
        ''', updates)
        
        self.conn.commit()
        print(f"✓ Updated GSAx for {len(updates)} goalies from shot data")
    
    def estimate_gsax_for_season_8(self):
        """
        Estimate GSAx for season 8 using save percentage vs league average
        """
        print("\n=== Estimating GSAx for 2025-26 Season ===")
        
        # Get league average save percentage from seasons with data
        self.cursor.execute('''
            SELECT AVG(save_percentage) 
            FROM goalie_stats 
            WHERE season_id IN (1, 5) AND games_played >= 5
        ''')
        league_avg_sv_pct = self.cursor.fetchone()[0] or 0.91
        print(f"League average save %: {league_avg_sv_pct:.3f}")
        
        # Get season 8 goalies
        self.cursor.execute('''
            SELECT player_id, team_id, season_id, shots_against, goals_against, save_percentage
            FROM goalie_stats
            WHERE season_id = 8 AND shots_against > 0
        ''')
        
        goalies = self.cursor.fetchall()
        print(f"Found {len(goalies)} goalies in season 8")
        
        updates = []
        for row in goalies:
            goalie_id, team_id, season_id, shots, goals, sv_pct = row
            
            if shots == 0:
                continue
            
            # Expected goals against based on league average
            expected_sv = league_avg_sv_pct
            expected_goals = shots * (1 - expected_sv)
            
            # GSAx = Expected Goals - Actual Goals
            gsax = expected_goals - goals
            
            updates.append((gsax, goalie_id, team_id, season_id))
        
        self.cursor.executemany('''
            UPDATE goalie_stats 
            SET gsax = ?
            WHERE player_id = ? AND team_id = ? AND season_id = ?
        ''', updates)
        
        self.conn.commit()
        print(f"✓ Updated estimated GSAx for {len(updates)} goalies")
    
    def show_gsax_leaders(self, season_id: int = 5, limit: int = 15):
        """Show GSAx leaders for a season"""
        self.cursor.execute('''
            SELECT season_name FROM seasons WHERE season_id = ?
        ''', (season_id,))
        result = self.cursor.fetchone()
        season_name = result[0] if result else f"Season {season_id}"
        
        print(f"\n=== {season_name} GSAx Leaders ===")
        print("(Goals Saved Above Expected - Higher is Better)")
        print()
        
        self.cursor.execute('''
            SELECT 
                p.first_name,
                p.last_name,
                t.name as team,
                gs.games_played,
                gs.wins,
                gs.save_percentage,
                gs.shots_against,
                gs.goals_against,
                ROUND(gs.gsax, 1) as gsax
            FROM goalie_stats gs
            JOIN players p ON gs.player_id = p.player_id
            JOIN teams t ON gs.team_id = t.team_id
            WHERE gs.season_id = ? AND gs.games_played >= 5
            ORDER BY gs.gsax DESC
            LIMIT ?
        ''', (season_id, limit))
        
        print(f"{'Rank':<5} {'Player':<25} {'Team':<18} {'GP':<4} {'W':<4} {'SV%':<6} {'SA':<5} {'GA':<4} {'GSAx':<6}")
        print("-" * 85)
        
        rank = 1
        for row in self.cursor.fetchall():
            name = f"{row[0]} {row[1]}"
            print(f"{rank:<5} {name:<25} {row[2]:<18} {row[3]:<4} {row[4]:<4} {row[5]:<6.3f} {row[6]:<5} {row[7]:<4} {row[8]:<+6}")
            rank += 1
    
    def show_all_seasons_summary(self):
        """Show GSAx summary for all seasons"""
        print("\n=== GSAx Summary by Season ===")
        
        self.cursor.execute('''
            SELECT 
                s.season_name,
                COUNT(*) as goalies,
                ROUND(AVG(gs.gsax), 2) as avg_gsax,
                ROUND(MAX(gs.gsax), 1) as max_gsax,
                ROUND(MIN(gs.gsax), 1) as min_gsax
            FROM goalie_stats gs
            JOIN seasons s ON gs.season_id = s.season_id
            WHERE gs.games_played >= 3
            GROUP BY gs.season_id
            ORDER BY gs.season_id
        ''')
        
        print(f"{'Season':<25} {'Goalies':<10} {'Avg GSAx':<12} {'Best':<10} {'Worst':<10}")
        print("-" * 70)
        for row in self.cursor.fetchall():
            print(f"{row[0]:<25} {row[1]:<10} {row[2]:<12} {row[3]:<10} {row[4]:<10}")
    
    def run(self):
        print("=== PWHL Goalie GSAx Calculator ===\n")
        
        # Calculate GSAx from shot data for seasons 1 & 5
        self.calculate_gsax_from_shots(1)
        self.calculate_gsax_from_shots(5)
        
        # Estimate GSAx for season 8
        self.estimate_gsax_for_season_8()
        
        # Show leaders
        self.show_gsax_leaders(season_id=5, limit=15)
        self.show_gsax_leaders(season_id=1, limit=10)
        self.show_gsax_leaders(season_id=8, limit=10)
        
        # Show summary
        self.show_all_seasons_summary()
        
        self.conn.close()
        print("\n✓ GSAx calculation complete!")

if __name__ == "__main__":
    calc = GSAxCalculator()
    calc.run()
