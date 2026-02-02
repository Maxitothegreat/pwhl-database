# PWHL Database

A comprehensive SQLite database for the Professional Women's Hockey League (PWHL), containing player statistics, game data, play-by-play information, and more for the 2023-24 and 2024-25 seasons.

## Overview

This project provides a normalized SQLite database of PWHL data scraped from the HockeyTech/LeagueStat API and play-by-play CSV data from the PWHL-Data-Reference repository.

## Data Sources

1. **HockeyTech/LeagueStat API** (`https://lscluster.hockeytech.com/feed/`)
   - API Key: `446521baf8c38984`
   - Client Code: `pwhl`
   - Provides: Seasons, teams, players, games, statistics

2. **PWHL-Data-Reference GitHub** (`https://github.com/IsabelleLefebvre97/PWHL-Data-Reference`)
   - Provides: Detailed play-by-play CSV files (shots, goals, penalties, faceoffs, hits, blocked shots)

## Database Schema

### Core Tables

#### `seasons`
- Season information (2023-24, 2024-25, etc.)
- Fields: season_id, season_name, shortname, career, playoff, start_date, end_date

#### `teams`
- Team information
- Fields: team_id, name, nickname, code, city, logo_url
- Teams: Boston Fleet, Minnesota Frost, Montréal Victoire, New York Sirens, Ottawa Charge, Toronto Sceptres

#### `players`
- Player biographical and profile information
- Fields: player_id, first_name, last_name, full_name, position, shoots, catches, height, birthdate, hometown, nationality, etc.

#### `roster_assignments`
- Links players to teams for specific seasons
- Fields: player_id, team_id, season_id, jersey_number, rookie, veteran, status

#### `games`
- Game schedule and results
- Fields: game_id, season_id, date_played, home_team_id, away_team_id, home_goals, away_goals, venue, attendance, etc.

### Statistics Tables

#### `skater_stats`
- Season-level statistics for skaters (forwards and defensemen)
- Includes: games_played, goals, assists, points, shots, shooting_percentage, power_play_stats, shorthanded_stats, faceoffs, hits, plus_minus, ice_time

#### `goalie_stats`
- Season-level statistics for goaltenders
- Includes: games_played, wins, losses, saves, save_percentage, GAA, shutouts, shootout_stats

#### `team_stats`
- Team standings and statistics per season
- Includes: wins, losses, points, goals_for, goals_against, power_play_percentage, penalty_kill_percentage, streaks

### Play-by-Play Tables

#### `shots`
- Individual shot attempts with location data
- Fields: game_id, player_id, goalie_id, period, x_location, y_location, shot_type, quality
- Useful for calculating Corsi/Fenwick and shot heat maps

#### `goals`
- Goal events with scorer and assist information
- Fields: game_id, scorer_id, assist1_id, assist2_id, period, goal_type (power_play, shorthanded, empty_net, etc.)

#### `penalties`
- Penalty calls
- Fields: game_id, player_id, minutes, penalty_class, penalty_description

#### `faceoffs`
- Faceoff events
- Fields: game_id, home_player_id, away_player_id, home_win, win_team_id, location

#### `hits`
- Body check events
- Fields: game_id, player_id, period, x_location, y_location

#### `blocked_shots`
- Shot blocking events
- Fields: game_id, blocker_id, shooter_id, team_id

#### `game_player_stats`
- Game-by-game player statistics (if available from API)
- Fields: game_id, player_id, goals, assists, shots, ice_time, etc.

## Database Statistics

| Table | Records | Description |
|-------|---------|-------------|
| seasons | 8 | All PWHL seasons including regular season, preseason, and playoffs |
| teams | 6 | All PWHL teams |
| players | 234 | All players who have appeared in PWHL |
| roster_assignments | 349 | Player-team-season combinations |
| games | 162 | Games from 2023-24 and 2024-25 regular seasons |
| skater_stats | 49 | Season statistics for skaters (API limitation) |
| goalie_stats | 5 | Season statistics for goalies |
| team_stats | 12 | Team standings per season |
| shots | 8,583 | Shot attempts with location data |
| goals | 745 | Goal events |
| penalties | 964 | Penalty calls |
| faceoffs | 8,224 | Faceoff events |
| hits | 3,717 | Body checks |
| blocked_shots | 2,812 | Shot blocks |

## Usage

### Python

```python
import sqlite3

# Connect to database
conn = sqlite3.connect('pwhl_database.db')
cursor = conn.cursor()

# Example: Get top 10 scorers from 2024-25 season
cursor.execute('''
    SELECT p.first_name, p.last_name, s.goals, s.assists, s.points
    FROM skater_stats s
    JOIN players p ON s.player_id = p.player_id
    WHERE s.season_id = 5
    ORDER BY s.points DESC
    LIMIT 10
''')

for row in cursor.fetchall():
    print(row)

conn.close()
```

### Sample Queries

```sql
-- Team standings for 2024-25 season
SELECT t.name, ts.wins, ts.losses, ts.points, ts.goals_for, ts.goals_against
FROM team_stats ts
JOIN teams t ON ts.team_id = t.team_id
WHERE ts.season_id = 5
ORDER BY ts.points DESC;

-- Shot heat map data for a specific player
SELECT x_location, y_location, is_goal
FROM shots
WHERE player_id = 13;  -- Hilary Knight

-- Faceoff win percentage by player
SELECT p.first_name, p.last_name, 
       COUNT(*) as total_faceoffs,
       SUM(CASE WHEN f.win_team_id = ra.team_id THEN 1 ELSE 0 END) as wins,
       ROUND(100.0 * SUM(CASE WHEN f.win_team_id = ra.team_id THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct
FROM faceoffs f
JOIN players p ON f.home_player_id = p.player_id OR f.away_player_id = p.player_id
JOIN roster_assignments ra ON p.player_id = ra.player_id
GROUP BY p.player_id
HAVING total_faceoffs > 100
ORDER BY win_pct DESC;
```

## Data Refresh

To update the database with fresh data:

```bash
python3 scrape_pwhl.py
```

This will:
1. Re-initialize the database from `schema.sql`
2. Fetch all data from the API and CSV sources
3. Populate all tables with current information

## Advanced Analytics Potential

With the shot location data in the `shots` table, you can calculate:

- **Corsi**: Shot attempts (shots + missed shots + blocked shots) for/against
- **Fenwick**: Unblocked shot attempts (shots + missed shots) for/against
- **Shot Heat Maps**: Visualize shooting patterns by location
- **Expected Goals (xG)**: Build models using shot location and type (though xG is not pre-calculated in this dataset)

## Schema Diagram

```
seasons
  │
  ├── teams
  │     └── team_stats (per season)
  │
  ├── players
  │     ├── roster_assignments (player-team-season)
  │     ├── skater_stats (per season)
  │     └── goalie_stats (per season)
  │
  └── games
        ├── shots
        ├── goals
        ├── penalties
        ├── faceoffs
        ├── hits
        ├── blocked_shots
        └── game_player_stats
```

## API Endpoints Used

### Seasons
- `feed=modulekit&view=seasons`

### Teams
- `feed=modulekit&view=teamsbyseason`

### Players
- `feed=modulekit&view=roster`
- GitHub: `all_players.csv`

### Games
- `feed=modulekit&view=schedule`

### Statistics
- `feed=modulekit&view=statviewtype&type=skaters`
- `feed=modulekit&view=statviewtype&type=goalies`
- `feed=modulekit&view=statviewtype&stat=conference&type=standings`

### Play-by-Play (GitHub CSV)
- `shots.csv` - All shot attempts
- `goals.csv` - Goal events
- `penalties.csv` - Penalty calls
- `faceoffs.csv` - Faceoff data
- `hits.csv` - Body checks
- `blocked_shots.csv` - Shot blocks

## Known Limitations

1. **Skater Stats API Limitation**: The HockeyTech API returns only ~25 top skaters per season for the `statviewtype` endpoint. For complete player statistics, consider aggregating from game-by-game data.

2. **Advanced Stats**: Corsi, Fenwick, and xG are not pre-calculated in this dataset. They can be derived from the play-by-play data in the `shots` table.

3. **Data Completeness**: Play-by-play data is only available for the 2023-24 season from the GitHub CSV source.

## License

Data is sourced from the official PWHL HockeyTech API and the PWHL-Data-Reference project. This database is provided for research and educational purposes.

## Contributing

To contribute improvements to the scraper or schema:

1. Fork the repository
2. Make your changes
3. Test the scraper: `python3 scrape_pwhl.py`
4. Submit a pull request

## Acknowledgments

- HockeyTech/LeagueStat for providing the official PWHL statistics API
- Isabelle Lefebvre for the PWHL-Data-Reference repository with play-by-play CSVs
- The Professional Women's Hockey League for making this data available
