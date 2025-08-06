import sqlite3
import pandas as pd
import os
from datetime import datetime

def create_database_schema(conn):
    """Create the database tables with proper schema"""
    cursor = conn.cursor()
    
    # Create games table (master list of games)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS games (
            appid INTEGER PRIMARY KEY,
            game_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create player_history table (main data table)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appid INTEGER,
            month TEXT NOT NULL,
            avg_players REAL NOT NULL,
            peak_players INTEGER NOT NULL,
            year INTEGER,
            month_num INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (appid) REFERENCES games (appid)
        )
    ''')
    
    # Create indexes for better query performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_appid ON player_history (appid)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_year ON player_history (year)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_month ON player_history (month)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_avg_players ON player_history (avg_players)')
    
    conn.commit()
    print("✓ Database schema created successfully")

def parse_month_year(month_str):
    """Extract year and month number from month string"""
    try:
        # Handle different month formats
        if "Last 30 Days" in month_str:
            return datetime.now().year, datetime.now().month
        
        # Parse formats like "January 2024", "July 2025"
        month_mapping = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        parts = month_str.strip().split()
        if len(parts) >= 2:
            month_name = parts[0]
            year = int(parts[1])
            month_num = month_mapping.get(month_name, 1)
            return year, month_num
        
        return None, None
    except:
        return None, None

def import_csv_to_sqlite(csv_file, db_file):
    """Import CSV data into SQLite database"""
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    print(f"📊 Loading data from {csv_file}...")
    df = pd.read_csv(csv_file)
    print(f"✓ Loaded {len(df)} records")
    
    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create database schema
    create_database_schema(conn)
    
    # Extract unique games
    print("📝 Processing games table...")
    games_df = df[['appid', 'game_name']].drop_duplicates()
    games_df.to_sql('games', conn, if_exists='replace', index=False)
    print(f"✓ Inserted {len(games_df)} unique games")
    
    # Process player history data
    print("📈 Processing player history data...")
    
    # Add year and month_num columns
    df[['year', 'month_num']] = df['month'].apply(
        lambda x: pd.Series(parse_month_year(x))
    )
    
    # Prepare history data (without game_name to avoid redundancy)
    history_df = df[['appid', 'month', 'avg_players', 'peak_players', 'year', 'month_num']].copy()
    
    # Remove rows with invalid dates
    history_df = history_df.dropna(subset=['year', 'month_num'])
    
    # Insert into database
    history_df.to_sql('player_history', conn, if_exists='replace', index=False)
    print(f"✓ Inserted {len(history_df)} player history records")
    
    # Create summary statistics
    create_summary_views(conn)
    
    conn.close()
    print(f"✅ Database created successfully: {db_file}")

def create_summary_views(conn):
    """Create useful views for analysis"""
    cursor = conn.cursor()
    
    # View: Latest data for each game
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS latest_player_data AS
        SELECT 
            g.game_name,
            g.appid,
            ph.month,
            ph.avg_players,
            ph.peak_players,
            ph.year
        FROM games g
        JOIN player_history ph ON g.appid = ph.appid
        WHERE (ph.year, ph.month_num) = (
            SELECT MAX(year), MAX(month_num) 
            FROM player_history ph2 
            WHERE ph2.appid = ph.appid
        )
        ORDER BY ph.avg_players DESC
    ''')
    
    # View: Top games by average players
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS top_games_avg AS
        SELECT 
            g.game_name,
            g.appid,
            AVG(ph.avg_players) as avg_avg_players,
            MAX(ph.peak_players) as max_peak_players,
            COUNT(*) as months_tracked
        FROM games g
        JOIN player_history ph ON g.appid = ph.appid
        GROUP BY g.appid, g.game_name
        ORDER BY avg_avg_players DESC
    ''')
    
    # View: Yearly trends
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS yearly_trends AS
        SELECT 
            g.game_name,
            ph.year,
            AVG(ph.avg_players) as avg_players_year,
            MAX(ph.peak_players) as peak_players_year
        FROM games g
        JOIN player_history ph ON g.appid = ph.appid
        WHERE ph.year IS NOT NULL
        GROUP BY g.appid, g.game_name, ph.year
        ORDER BY g.game_name, ph.year
    ''')
    
    conn.commit()
    print("✓ Created summary views for analysis")

def show_database_info(db_file):
    """Display information about the created database"""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    print("\n" + "="*50)
    print("📊 DATABASE SUMMARY")
    print("="*50)
    
    # Games count
    cursor.execute("SELECT COUNT(*) FROM games")
    games_count = cursor.fetchone()[0]
    print(f"🎮 Total games: {games_count}")
    
    # Records count
    cursor.execute("SELECT COUNT(*) FROM player_history")
    records_count = cursor.fetchone()[0]
    print(f"📈 Total player records: {records_count}")
    
    # Date range
    cursor.execute("SELECT MIN(year), MAX(year) FROM player_history WHERE year IS NOT NULL")
    min_year, max_year = cursor.fetchone()
    print(f"📅 Date range: {min_year} - {max_year}")
    
    # Top 5 games by latest average players
    print(f"\n🏆 TOP 5 GAMES (Latest Average Players):")
    cursor.execute("""
        SELECT game_name, avg_players 
        FROM latest_player_data 
        LIMIT 5
    """)
    
    for i, (game, avg_players) in enumerate(cursor.fetchall(), 1):
        print(f"  {i}. {game}: {avg_players:,.0f} avg players")
    
    conn.close()

if __name__ == "__main__":
    print("🗄️  Steam Data CSV to SQLite Converter")
    print("="*40)
    
    # Check for existing CSV files
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv') and 'steam' in f.lower()]
    
    if not csv_files:
        print("❌ No Steam CSV files found in current directory")
        exit()
    
    print("📁 Found CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"  {i}. {file}")
    
    # Let user choose file or use the most recent
    if len(csv_files) == 1:
        csv_file = csv_files[0]
        print(f"✓ Using: {csv_file}")
    else:
        choice = input(f"\nEnter file number (1-{len(csv_files)}) or press Enter for newest: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(csv_files):
            csv_file = csv_files[int(choice) - 1]
        else:
            # Use the most recent file
            csv_file = max(csv_files, key=os.path.getmtime)
            print(f"✓ Using newest file: {csv_file}")
    
    # Create database filename
    db_file = csv_file.replace('.csv', '.db')
    
    print(f"\n🔄 Converting {csv_file} → {db_file}")
    
    # Import data
    import_csv_to_sqlite(csv_file, db_file)
    
    # Show summary
    show_database_info(db_file)
    
    print(f"\n✅ Conversion complete!")
    print(f"💡 You can now query the database using:")
    print(f"   sqlite3 {db_file}")
    print(f"   or connect with pandas: pd.read_sql('SELECT * FROM games', sqlite3.connect('{db_file}'))")
