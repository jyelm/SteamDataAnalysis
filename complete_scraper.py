import requests
import pandas as pd
import time
from bs4 import BeautifulSoup

def fetch_history(appid, game_name):
    """Fetch history for a game and include the game name"""
    page = f"https://steamcharts.com/app/{appid}"
    
    try:
        response = requests.get(page, headers={"User-Agent":"Mozilla/5.0"})
        if response.status_code != 200:
            print(f"Failed to fetch {game_name} ({appid}): Status {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.text, "html.parser")
        rows = []
        
        # Use correct selector: table.common-table
        for tr in soup.select("table.common-table tr")[1:]:  # skip header
            cells = [c.get_text(strip=True).replace(',', '') for c in tr('td')]
            if len(cells) >= 5:  # month | avg | gain | percent | peak
                try:
                    month = cells[0]
                    avg_players = float(cells[1]) if cells[1] and cells[1] != '-' else 0
                    peak_players = int(cells[4]) if cells[4] and cells[4] != '-' else 0
                    # Include game name in the tuple
                    rows.append((appid, game_name, month, avg_players, peak_players))
                except (ValueError, IndexError) as e:
                    print(f"Error parsing row for {game_name} ({appid}): {cells} - {e}")
                    continue
        
        # Add small delay to be respectful to the server
        time.sleep(0.5)
        return rows
        
    except Exception as e:
        print(f"Error fetching {game_name} ({appid}): {e}")
        return []

def scrape_all_games(limit=None):
    """Scrape all games or limit to a specific number"""
    # Get top games list
    print("Fetching top games list...")
    url = "https://steamcharts.com/top"
    soup = BeautifulSoup(requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text, "html.parser")
    games = [(int(a['href'].split('/')[-1]), a.text.strip()) 
             for a in soup.select("table#top-games a[href^='/app/']")]
    
    print(f"Found {len(games)} games")
    
    # Apply limit if specified
    if limit:
        games = games[:limit]
        print(f"Processing first {limit} games")
    
    # Fetch history for all games
    hist = []
    for i, (appid, name) in enumerate(games, 1):
        print(f"[{i}/{len(games)}] Fetching history for {name} ({appid})...")
        game_hist = fetch_history(appid, name)
        hist.extend(game_hist)
        print(f"  → Got {len(game_hist)} records")
    
    # Create DataFrame with game names
    df = pd.DataFrame(hist, columns=["appid", "game_name", "month", "avg_players", "peak_players"])
    print(f"\nTotal records: {len(df)}")
    
    return df

if __name__ == "__main__":
    # Ask user how many games to process
    print("Steam Data Scraper")
    print("1. Test with 5 games")
    print("2. Process all top 100 games (takes ~1 minute)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        df = scrape_all_games(limit=5)
        filename = "steam_top5_with_names.csv"
    else:
        df = scrape_all_games()  # No limit = all games
        filename = "steam_top100_with_names.csv"
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"\nSaved to {filename}")
    
    # Show preview
    print("\nPreview:")
    print(df.head(10))
    
    # Show summary by game
    print(f"\nGames processed:")
    game_counts = df.groupby('game_name').size().sort_values(ascending=False)
    for game, count in game_counts.head(10).items():
        print(f"  {game}: {count} records")
