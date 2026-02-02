# GitHub Publishing - Manual Steps

Your PWHL database is ready to push! Since GitHub CLI auth is being tricky, here are the quickest options:

## Option 1: Quick Web Upload (Fastest)

1. Go to https://github.com/new
2. Name: `pwhl-database`
3. Description: "Comprehensive SQLite database for the Professional Women's Hockey League"
4. **Don't** initialize with README
5. Click **Create repository**

Then copy-paste these commands in your terminal:

```bash
cd /home/molt/.openclaw/workspace/pwhl-database
git remote add origin https://github.com/YOUR_USERNAME/pwhl-database.git
git branch -m main
git push -u origin main
```

## Option 2: GitHub CLI Token

Create a Personal Access Token:
1. https://github.com/settings/tokens/new
2. Check "repo" scope
3. Generate token
4. Give it to me and I'll auth with it

## What's In The Repo

- `pwhl_database.db` (2.5MB) - The actual database
- `schema.sql` - Table definitions
- `scrape_pwhl.py` - Data collection script
- `README.md` - Full documentation with sample queries
- 234 players, 162 games, 8,500+ shots with coordinates

Want me to try a different auth approach or are you good to run those git commands?