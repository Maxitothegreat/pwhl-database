================================================================================
PWHL DATABASE - COMPREHENSIVE SUMMARY REPORT
================================================================================
Generated: 2026-02-02 22:21:50

📊 DATABASE OVERVIEW
----------------------------------------
  Players                        234
  Teams                            8
  Games                          223
  Skater Season Stats          1,104
  Goalie Season Stats            119
  Shot Records                 8,583
  Faceoffs                     8,224
  Hits                         3,717
  Blocked Shots                2,812
  Penalties                      964
  Head-to-Head Records            56
  Venue Stats                     26

📅 SEASONS BREAKDOWN
----------------------------------------
  Season               Games    Skaters    Goalies 
  --------------------------------------------------
  2024 Regular Season  72       134        13      
  2024 Preseason       0        135        18      
  2024 Playoffs        0        80         5       
  2024-25 Preseason    0        141        19      
  2024-25 Regular Season 90       139        16      
  2025 Playoffs        0        82         6       
  2025-26 Preseason    0        194        25      
  2025-26 Regular Season 61       173        16      

🔬 ADVANCED STATISTICS AVAILABLE
----------------------------------------
  xG (Expected Goals)            408 records
  GSAx (Goalie)                  119 records
  Points per 60                  656 records
  Game Score                     990 records
  Team PDO                        12 records
  Corsi %                         12 records
  Faceoff %                        0 records

🏆 2024-25 SEASON LEADERS
----------------------------------------
  Points:
    1. Hilary Knight (Boston Fleet): 29
    2. Sarah Fillier (New York Sirens): 29
    3. Daryl Watts (Toronto Sceptres): 27

  Goals:
    1. Marie-Philip Poulin (Montréal Victoire): 19
    2. Hilary Knight (Boston Fleet): 15
    3. Tereza Vanišová (Ottawa Charge): 15

  Expected Goals (xG):
    1. Hilary Knight (Boston Fleet): 9.7
    2. Tereza Vanišová (Ottawa Charge): 9.2
    3. Laura Stacey (Montréal Victoire): 9.0

  Goals Saved Above Expected (GSAx):
    1. Ann-Renée Desbiens (Montréal Victoire): +6.9
    2. Aerin Frankel (Boston Fleet): +5.1
    3. Emerance Maschmeyer (Ottawa Charge): +1.4

⚠️  DATA QUALITY NOTES
----------------------------------------
  • Seasons 1 & 5: Full shot-level play-by-play data available
  • Season 8: Estimated xG (no shot coordinates available)
  • Faceoff data: Available for seasons 1 & 5 only
  • Ice time: Estimated when not officially tracked

📁 KEY FILES IN REPOSITORY
----------------------------------------
  pwhl_database.db               Main SQLite database (2.5MB+)
  schema.sql                     Database schema definition
  scrape_pwhl.py                 Main data scraper
  calculate_xg.py                Expected Goals calculator
  calculate_gsax.py              Goalie GSAx calculator
  calculate_advanced_stats.py    Advanced stats (Corsi, PDO, Game Score)
  additional_analytics.py        Streaks, venue, head-to-head analysis
  LIMITATIONS.md                 Known data limitations

================================================================================
Database is ready for website integration!
================================================================================