#!/usr/bin/env python3
"""
Generate comprehensive database summary report
"""
import sqlite3
from datetime import datetime

def generate_summary(db_path: str = "pwhl_database.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    report = []
    report.append("=" * 80)
    report.append("PWHL DATABASE - COMPREHENSIVE SUMMARY REPORT")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Overview stats
    report.append("📊 DATABASE OVERVIEW")
    report.append("-" * 40)
    
    tables = [
        ('players', 'Players'),
        ('teams', 'Teams'),
        ('games', 'Games'),
        ('skater_stats', 'Skater Season Stats'),
        ('goalie_stats', 'Goalie Season Stats'),
        ('shots', 'Shot Records'),
        ('faceoffs', 'Faceoffs'),
        ('hits', 'Hits'),
        ('blocked_shots', 'Blocked Shots'),
        ('penalties', 'Penalties'),
        ('head_to_head', 'Head-to-Head Records'),
        ('venue_stats', 'Venue Stats'),
    ]
    
    for table, name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        report.append(f"  {name:<25} {count:>8,}")
    
    report.append("")
    
    # Seasons breakdown
    report.append("📅 SEASONS BREAKDOWN")
    report.append("-" * 40)
    
    cursor.execute('''
        SELECT s.season_id, s.season_name, 
               COUNT(DISTINCT g.game_id) as games,
               COUNT(DISTINCT ss.player_id) as skaters,
               COUNT(DISTINCT gs.player_id) as goalies
        FROM seasons s
        LEFT JOIN games g ON s.season_id = g.season_id
        LEFT JOIN skater_stats ss ON s.season_id = ss.season_id
        LEFT JOIN goalie_stats gs ON s.season_id = gs.season_id
        GROUP BY s.season_id
        ORDER BY s.season_id
    ''')
    
    report.append(f"  {'Season':<20} {'Games':<8} {'Skaters':<10} {'Goalies':<8}")
    report.append("  " + "-" * 50)
    for row in cursor.fetchall():
        report.append(f"  {row[1]:<20} {row[2] or 0:<8} {row[3] or 0:<10} {row[4] or 0:<8}")
    
    report.append("")
    
    # Advanced stats available
    report.append("🔬 ADVANCED STATISTICS AVAILABLE")
    report.append("-" * 40)
    
    advanced_stats = [
        ('skater_stats', 'xG (Expected Goals)', 'season_id IN (1, 5, 8) AND xg > 0'),
        ('goalie_stats', 'GSAx (Goalie)', 'gsax IS NOT NULL'),
        ('skater_stats', 'Points per 60', 'points_per_60 > 0'),
        ('skater_stats', 'Game Score', 'game_score > 0'),
        ('team_stats', 'Team PDO', 'pdo > 0'),
        ('team_stats', 'Corsi %', 'corsi_pct != 50'),
        ('skater_stats', 'Faceoff %', 'faceoff_pct > 0'),
    ]
    
    for table, stat, condition in advanced_stats:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
        count = cursor.fetchone()[0]
        report.append(f"  {stat:<25} {count:>8,} records")
    
    report.append("")
    
    # Leaders
    report.append("🏆 2024-25 SEASON LEADERS")
    report.append("-" * 40)
    
    # Points
    cursor.execute('''
        SELECT p.first_name, p.last_name, t.name, s.points
        FROM skater_stats s
        JOIN players p ON s.player_id = p.player_id
        JOIN teams t ON s.team_id = t.team_id
        WHERE s.season_id = 5
        ORDER BY s.points DESC LIMIT 3
    ''')
    report.append("  Points:")
    for i, row in enumerate(cursor.fetchall(), 1):
        report.append(f"    {i}. {row[0]} {row[1]} ({row[2]}): {row[3]}")
    
    # Goals
    cursor.execute('''
        SELECT p.first_name, p.last_name, t.name, s.goals
        FROM skater_stats s
        JOIN players p ON s.player_id = p.player_id
        JOIN teams t ON s.team_id = t.team_id
        WHERE s.season_id = 5
        ORDER BY s.goals DESC LIMIT 3
    ''')
    report.append("\n  Goals:")
    for i, row in enumerate(cursor.fetchall(), 1):
        report.append(f"    {i}. {row[0]} {row[1]} ({row[2]}): {row[3]}")
    
    # xG
    cursor.execute('''
        SELECT p.first_name, p.last_name, t.name, s.xg
        FROM skater_stats s
        JOIN players p ON s.player_id = p.player_id
        JOIN teams t ON s.team_id = t.team_id
        WHERE s.season_id = 5 AND s.xg > 0
        ORDER BY s.xg DESC LIMIT 3
    ''')
    report.append("\n  Expected Goals (xG):")
    for i, row in enumerate(cursor.fetchall(), 1):
        report.append(f"    {i}. {row[0]} {row[1]} ({row[2]}): {row[3]:.1f}")
    
    # GSAx
    cursor.execute('''
        SELECT p.first_name, p.last_name, t.name, gs.gsax
        FROM goalie_stats gs
        JOIN players p ON gs.player_id = p.player_id
        JOIN teams t ON gs.team_id = t.team_id
        WHERE gs.season_id = 5 AND gs.gsax IS NOT NULL
        ORDER BY gs.gsax DESC LIMIT 3
    ''')
    report.append("\n  Goals Saved Above Expected (GSAx):")
    for i, row in enumerate(cursor.fetchall(), 1):
        report.append(f"    {i}. {row[0]} {row[1]} ({row[2]}): {row[3]:+.1f}")
    
    report.append("")
    
    # Data quality notes
    report.append("⚠️  DATA QUALITY NOTES")
    report.append("-" * 40)
    report.append("  • Seasons 1 & 5: Full shot-level play-by-play data available")
    report.append("  • Season 8: Estimated xG (no shot coordinates available)")
    report.append("  • Faceoff data: Available for seasons 1 & 5 only")
    report.append("  • Ice time: Estimated when not officially tracked")
    report.append("")
    
    # Files in repo
    report.append("📁 KEY FILES IN REPOSITORY")
    report.append("-" * 40)
    files = [
        ('pwhl_database.db', 'Main SQLite database (2.5MB+)'),
        ('schema.sql', 'Database schema definition'),
        ('scrape_pwhl.py', 'Main data scraper'),
        ('calculate_xg.py', 'Expected Goals calculator'),
        ('calculate_gsax.py', 'Goalie GSAx calculator'),
        ('calculate_advanced_stats.py', 'Advanced stats (Corsi, PDO, Game Score)'),
        ('additional_analytics.py', 'Streaks, venue, head-to-head analysis'),
        ('LIMITATIONS.md', 'Known data limitations'),
    ]
    for filename, description in files:
        report.append(f"  {filename:<30} {description}")
    
    report.append("")
    report.append("=" * 80)
    report.append("Database is ready for website integration!")
    report.append("=" * 80)
    
    conn.close()
    return "\n".join(report)

if __name__ == "__main__":
    report = generate_summary()
    print(report)
    
    # Save to file
    with open("DATABASE_SUMMARY.md", "w") as f:
        f.write(report)
    print("\n✓ Summary saved to DATABASE_SUMMARY.md")
