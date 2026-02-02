# PWHL Database Project - Summary

## Project Completed Successfully ✓

A comprehensive SQLite database for the Professional Women's Hockey League (PWHL) has been created with full data from the 2023-24 and 2024-25 seasons.

## Deliverables

All required deliverables have been created in `/home/molt/.openclaw/workspace/pwhl-database/`:

### Core Files
1. **pwhl_database.db** (2.5MB) - SQLite database with all PWHL data
2. **schema.sql** (17KB) - Complete database schema definition
3. **scrape_pwhl.py** (39KB) - Python scraper for data collection
4. **README.md** (8.2KB) - Comprehensive documentation
5. **requirements.txt** - Python dependencies
6. **LICENSE** - MIT License
7. **setup.sh** - Setup script for easy installation
8. **GITHUB_SETUP.md** - Instructions for publishing to GitHub

## Data Coverage

### Seasons
- Season 1 (2023-24 Regular Season): 72 games
- Season 5 (2024-25 Regular Season): 90 games
- Plus: Preseason and Playoff seasons in database

### Teams (6)
1. Boston Fleet (BOS)
2. Minnesota Frost (MIN)
3. Montréal Victoire (MTL)
4. New York Sirens (NY)
5. Ottawa Charge (OTT)
6. Toronto Sceptres (TOR)

### Database Statistics

| Table | Records | Description |
|-------|---------|-------------|
| seasons | 8 | All seasons including regular, preseason, playoffs |
| teams | 6 | All PWHL teams |
| players | 234 | All players with complete profiles |
| roster_assignments | 349 | Player-team-season combinations |
| games | 162 | Complete game records |
| skater_stats | 49 | Season statistics (API limited to top players) |
| goalie_stats | 5 | Goalie season statistics |
| team_stats | 12 | Team standings per season |
| shots | 8,583 | Shot attempts with x,y coordinates |
| goals | 745 | Goal events with assists |
| penalties | 964 | Penalty calls |
| faceoffs | 8,224 | Faceoff events with locations |
| hits | 3,717 | Body check events |
| blocked_shots | 2,812 | Shot blocking events |

## Schema Design

### Normalized Structure
- **Core entities**: seasons, teams, players
- **Relationships**: roster_assignments (player-team-season)
- **Events**: games
- **Statistics**: skater_stats, goalie_stats, team_stats
- **Play-by-Play**: shots, goals, penalties, faceoffs, hits, blocked_shots

### Foreign Key Relationships
All tables properly reference each other:
- `games.season_id` → `seasons.season_id`
- `games.home_team_id` → `teams.team_id`
- `skater_stats.player_id` → `players.player_id`
- All play-by-play tables link to games, players, and teams

## Data Sources

### 1. HockeyTech/LeagueStat API
- Base URL: `https://lscluster.hockeytech.com/feed/`
- Key: `446521baf8c38984`
- Client: `pwhl`

Endpoints Used:
- `modulekit/seasons` - Season information
- `modulekit/teamsbyseason` - Team data
- `modulekit/roster` - Player rosters
- `modulekit/schedule` - Game schedules
- `modulekit/statviewtype` - Player & team statistics

### 2. GitHub CSV Data
- Source: `https://github.com/IsabelleLefebvre97/PWHL-Data-Reference`
- Play-by-play CSVs: shots, goals, penalties, faceoffs, hits, blocked_shots

## Usage Examples

### Query Top Scorers
```sql
SELECT p.first_name || ' ' || p.last_name as name, 
       s.goals, s.assists, s.points
FROM skater_stats s
JOIN players p ON s.player_id = p.player_id
WHERE s.season_id = 5
ORDER BY s.points DESC
LIMIT 10;
```

Result:
- Hilary Knight: 15G, 14A, 29PTS
- Alina Müller: 7G, 12A, 19PTS
- Susanna Tapani: 11G, 7A, 18PTS

### Shot Heat Map Data
```sql
SELECT x_location, y_location, is_goal
FROM shots
WHERE player_id = 13;  -- Hilary Knight
```

### Team Standings
```sql
SELECT t.name, ts.wins, ts.losses, ts.points
FROM team_stats ts
JOIN teams t ON ts.team_id = t.team_id
WHERE ts.season_id = 5
ORDER BY ts.points DESC;
```

## Advanced Analytics Potential

The database includes shot location data (x, y coordinates) enabling:

1. **Corsi/Fenwick Calculation**
   - Use `shots` table to calculate shot attempts
   - Filter by team and game state

2. **Shot Heat Maps**
   - Aggregate shots by location
   - Visualize shooting patterns

3. **Expected Goals (xG)**
   - Build models using shot location, type, and quality
   - Not pre-calculated but derivable from available data

## Known Limitations

1. **Skater Stats API Limitation**: The HockeyTech API returns only ~25 top skaters per season for aggregate statistics. Complete player statistics would need to be calculated from game-by-game data.

2. **Play-by-Play Coverage**: Full play-by-play data (shots, goals, etc.) is primarily available for the 2023-24 season from the GitHub CSV source.

3. **Advanced Stats**: Corsi, Fenwick, and xG are not pre-calculated but can be derived from the raw data.

## How to Use

### Quick Start
```bash
cd /home/molt/.openclaw/workspace/pwhl-database
sqlite3 pwhl_database.db
```

### Refresh Data
```bash
python3 scrape_pwhl.py
```

### Python Integration
```python
import sqlite3

conn = sqlite3.connect('pwhl_database.db')
cursor = conn.cursor()

# Your queries here

cursor.close()
```

## GitHub Repository Setup

To publish this to GitHub:

1. See `GITHUB_SETUP.md` for detailed instructions
2. Quick option:
```bash
cd /home/molt/.openclaw/workspace/pwhl-database
git remote add origin https://github.com/YOUR_USERNAME/pwhl-database.git
git branch -m main
git push -u origin main
```

## Technical Details

### Dependencies
- Python 3.7+
- requests library
- sqlite3 (built-in)

### Database Size
- 2.5 MB (fits easily in GitHub's 100MB limit)

### Code Quality
- Comprehensive error handling
- Rate limiting for API calls
- Graceful handling of missing data
- Type hints and documentation

## Future Enhancements

Possible additions for future versions:
1. Game-by-game player statistics aggregation
2. Pre-calculated advanced stats (Corsi, Fenwick)
3. Playoff bracket data
4. Player transaction history
5. Prospect/draft data
6. Expected Goals (xG) model

## Attribution

- Data: Official PWHL HockeyTech API
- Play-by-Play: PWHL-Data-Reference by Isabelle Lefebvre
- This project is not affiliated with or endorsed by the PWHL

---

**Project Status**: ✓ Complete and Ready for Use

**Last Updated**: February 2, 2025
