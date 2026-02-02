#!/bin/bash
# Setup script for PWHL Database

echo "Setting up PWHL Database..."

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pip3 install -r requirements.txt

# Run scraper to verify everything works
echo ""
echo "Running scraper to verify setup..."
python3 scrape_pwhl.py

echo ""
echo "Setup complete! Database is ready at pwhl_database.db"
echo ""
echo "To query the database:"
echo "  sqlite3 pwhl_database.db"
echo ""
echo "Or use Python:"
echo "  python3 -c \"import sqlite3; conn = sqlite3.connect('pwhl_database.db'); cursor = conn.cursor(); cursor.execute('SELECT * FROM teams'); print(cursor.fetchall())\""
