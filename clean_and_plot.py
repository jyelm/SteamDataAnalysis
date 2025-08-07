import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sqlite3

def clean_game_data(df, min_avg_players=500, min_consecutive_months=3):
    """
    Clean game data by finding the first meaningful data point and filtering from there.
    
    Args:
        df: DataFrame with game data
        min_avg_players: Minimum average players to consider as meaningful
        min_consecutive_months: Number of consecutive months above threshold
    """
    cleaned_games = []
    
    for game_name in df['game_name'].unique():
        game_data = df[df['game_name'] == game_name].copy()
        
        # Sort by date (convert month to datetime for proper sorting)
        game_data['date'] = pd.to_datetime(game_data['month'], format='%B %Y', errors='coerce')
        game_data = game_data.sort_values('date').reset_index(drop=True)
        
        # Find first meaningful data point
        meaningful_start = None
        consecutive_count = 0
        
        for idx, row in game_data.iterrows():
            if row['avg_players'] >= min_avg_players:
                consecutive_count += 1
                if consecutive_count >= min_consecutive_months and meaningful_start is None:
                    meaningful_start = max(0, idx - min_consecutive_months + 1)
                    break
            else:
                consecutive_count = 0
        
        # If we found a meaningful start point, keep data from there
        if meaningful_start is not None:
            cleaned_data = game_data.iloc[meaningful_start:].copy()
            cleaned_games.append(cleaned_data)
        else:
            # If no meaningful data found, keep original (might be a newer game)
            cleaned_games.append(game_data)
    
    return pd.concat(cleaned_games, ignore_index=True) if cleaned_games else df

def plot_game_trends(df, games_to_plot=None, save_plot=True):
    """
    Plot clean line graphs for specified games.
    """
    if games_to_plot is None:
        games_to_plot = ['Warframe', 'War Thunder', 'Counter-Strike 2', 'Dota 2']
    
    plt.figure(figsize=(15, 10))
    
    for i, game_name in enumerate(games_to_plot, 1):
        game_data = df[df['game_name'] == game_name].copy()
        
        if game_data.empty:
            print(f"No data found for {game_name}")
            continue
            
        # Convert month to datetime and sort
        game_data['date'] = pd.to_datetime(game_data['month'], format='%B %Y', errors='coerce')
        game_data = game_data.sort_values('date')
        
        # Create subplot
        plt.subplot(2, 2, i)
        plt.plot(game_data['date'], game_data['avg_players'], linewidth=2, label=f'{game_name} Avg Players')
        plt.plot(game_data['date'], game_data['peak_players'], linewidth=1, alpha=0.7, label=f'{game_name} Peak Players')
        
        plt.title(f'{game_name} Player Trends (Cleaned Data)', fontsize=14)
        plt.ylabel('Player Count')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Clean up x-axis
        plt.xticks(rotation=45)
        
        # Show data range info
        start_date = game_data['date'].min().strftime('%B %Y')
        end_date = game_data['date'].max().strftime('%B %Y')
        plt.text(0.02, 0.98, f'Data: {start_date} to {end_date}', 
                transform=plt.gca().transAxes, verticalalignment='top', 
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    
    if save_plot:
        plt.savefig('cleaned_player_trends.png', dpi=300, bbox_inches='tight')
        print("Plot saved as 'cleaned_player_trends.png'")
    
    plt.show()

def main():
    # Option 1: Load from CSV
    print("Loading data from CSV...")
    df = pd.read_csv('steam_top100_with_names.csv')
    
    print(f"Original data shape: {df.shape}")
    print(f"Games in dataset: {df['game_name'].nunique()}")
    
    # Clean the data
    print("\nCleaning data...")
    cleaned_df = clean_game_data(df, min_avg_players=500, min_consecutive_months=2)
    
    print(f"Cleaned data shape: {cleaned_df.shape}")
    print(f"Data points removed: {df.shape[0] - cleaned_df.shape[0]}")
    
    # Show before/after for Warframe
    print("\n=== WARFRAME BEFORE/AFTER CLEANING ===")
    warframe_original = df[df['game_name'] == 'Warframe'].sort_values('month')
    warframe_cleaned = cleaned_df[cleaned_df['game_name'] == 'Warframe'].sort_values('month')
    
    print("Original first 5 entries:")
    print(warframe_original[['month', 'avg_players', 'peak_players']].head())
    
    print("\nCleaned first 5 entries:")
    print(warframe_cleaned[['month', 'avg_players', 'peak_players']].head())
    
    # Plot the results
    print("\nGenerating plots...")
    plot_game_trends(cleaned_df)
    
    # Save cleaned data
    cleaned_df.to_csv('steam_data_cleaned.csv', index=False)
    print("\nCleaned data saved as 'steam_data_cleaned.csv'")

if __name__ == "__main__":
    main()
