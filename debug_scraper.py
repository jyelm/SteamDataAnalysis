import requests
import time
from bs4 import BeautifulSoup

def debug_single_game(appid):
    """Debug a single game to see what's happening"""
    print(f"Debugging app ID: {appid}")
    
    page = f"https://steamcharts.com/app/{appid}"
    print(f"URL: {page}")
    
    try:
        response = requests.get(page, headers={"User-Agent":"Mozilla/5.0"})
        print(f"Status code: {response.status_code}")
        
        if response.status_code != 200:
            print("Failed to fetch page!")
            return []
            
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Check if the table exists
        table = soup.select("table#app-chart")
        print(f"Found tables with #app-chart: {len(table)}")
        
        if not table:
            # Try alternative selectors
            all_tables = soup.find_all("table")
            print(f"Total tables found: {len(all_tables)}")
            
            for i, t in enumerate(all_tables):
                print(f"Table {i}: {t.get('id', 'no-id')} - {t.get('class', 'no-class')}")
            
            return []
        
        # Check rows
        rows = soup.select("table#app-chart tr")
        print(f"Found rows: {len(rows)}")
        
        if len(rows) > 1:
            # Print first few rows for inspection
            for i, row in enumerate(rows[:3]):
                cells = [c.get_text(strip=True) for c in row('td')]
                print(f"Row {i}: {cells}")
        
        # Try to extract data
        data_rows = []
        for tr in rows[1:]:  # skip header
            cells = [c.get_text(strip=True).replace(',', '') for c in tr('td')]
            print(f"Processing cells: {cells}")
            
            if len(cells) >= 4:
                try:
                    month = cells[0]
                    avg_players = float(cells[1]) if cells[1] else 0
                    peak_players = int(cells[3]) if cells[3] else 0
                    data_rows.append((appid, month, avg_players, peak_players))
                    print(f"Successfully parsed: {month}, {avg_players}, {peak_players}")
                except (ValueError, IndexError) as e:
                    print(f"Error parsing row: {e}")
                    continue
        
        return data_rows
        
    except Exception as e:
        print(f"Error fetching {appid}: {e}")
        return []

# Test with Counter-Strike 2 (app ID 730)
if __name__ == "__main__":
    debug_single_game(730)
