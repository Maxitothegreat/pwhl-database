# PWHL Database - Known Limitations

## xG (Expected Goals) Data Availability

### Seasons with Full xG Data
- **2023-24 Regular Season (Season 1)**: ✅ 4,157 shots analyzed
- **2024-25 Regular Season (Season 5)**: ✅ 4,426 shots analyzed
- **Playoff seasons (3, 6)**: Limited data

### Seasons with Estimated xG Only
- **2025-26 Regular Season (Season 8)**: ⚠️ Estimated xG only
  - **Reason**: Shot-level play-by-play data with coordinates is not publicly available via API
  - **Firebase API**: Requires authentication (permission denied)
  - **HockeyTech endpoints**: Now return "Undefined" for play-by-play
  - **GitHub CSV repo**: Has goals/blocked shots but no comprehensive "all shots" file

### Estimation Method for 2025-26
When shot-level data is unavailable, xG is estimated using:
- Shot volume (total shots)
- Shooting percentage
- Shot type distribution (if available)
- Historical player/team patterns from previous seasons

### Recommendation
For accurate xG analysis, use seasons 1 and 5. Season 8 xG should be treated as approximate.

---
*Last updated: 2026-02-02*