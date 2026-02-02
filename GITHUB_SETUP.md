# GitHub Setup Instructions

## Creating a GitHub Repository

Since the GitHub CLI is not authenticated, follow these steps to push this repository to GitHub:

### Option 1: Using GitHub Website

1. Go to https://github.com/new
2. Enter repository name: `pwhl-database`
3. Add description: "Comprehensive SQLite database for the Professional Women's Hockey League (PWHL)"
4. Make it Public (or Private if preferred)
5. Do NOT initialize with README (we already have one)
6. Click "Create repository"

### Option 2: Push Existing Code

After creating the repository on GitHub, run these commands in the pwhl-database directory:

```bash
cd /home/molt/.openclaw/workspace/pwhl-database

# Add the remote repository (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/pwhl-database.git

# Rename branch to main
git branch -m main

# Push to GitHub
git push -u origin main
```

### Option 3: Using GitHub CLI (if authenticated)

```bash
cd /home/molt/.openclaw/workspace/pwhl-database
gh repo create pwhl-database --public --source=. --push
```

## Files Included

- `pwhl_database.db` - The SQLite database (2.5MB)
- `schema.sql` - Database schema definition
- `scrape_pwhl.py` - Python scraper script
- `README.md` - Documentation
- `requirements.txt` - Python dependencies
- `LICENSE` - MIT License
- `.gitignore` - Git ignore file
- `setup.sh` - Setup script

## Database Size Note

The database file (pwhl_database.db) is approximately 2.5MB. GitHub has a 100MB file size limit, so this can be committed directly to the repository.

## Data Attribution

When publishing, include this attribution:

> Data sourced from the official PWHL HockeyTech API and the PWHL-Data-Reference project by Isabelle Lefebvre. This project is not affiliated with or endorsed by the Professional Women's Hockey League.
