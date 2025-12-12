"""
Fetch stock price data for companies in the roster.
Retrieves opening prices and calculates daily and 7-day percentage changes.
Stores 30 days of historical price data and trading volume for charting.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path

# Set up paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
ROSTER_PATH = PROJECT_ROOT / 'rosters' / 'main-roster.csv'
OUTPUT_DIR = PROJECT_ROOT / 'data' / 'stock_prices'

def fetch_stock_data():
    """
    Fetch stock data for all companies in the roster.
    Returns a DataFrame with ticker, company, opening price, daily change %, 7-day change %, 
    90-day price history, and trading volume history.
    """
    print("Loading roster...")
    roster_df = pd.read_csv(ROSTER_PATH)
    
    # Filter out companies without stock tickers (NA or missing)
    companies_with_stocks = roster_df[
        (roster_df['Stock'].notna()) & 
        (roster_df['Stock'] != 'NA') &
        (roster_df['Stock'] != 'N/A') &
        (roster_df['Stock'] != '#N/A')
    ].copy()
    
    print(f"Found {len(companies_with_stocks)} companies with stock tickers")
    
    results = []
    failed_tickers = []
    
    for idx, row in companies_with_stocks.iterrows():
        ticker = row['Stock']
        company = row['Company']
        
        try:
            print(f"Fetching data for {company} ({ticker})...")
            
            # Create ticker object
            stock = yf.Ticker(ticker)
            
            # Get ~120 days of history to ensure we have 90 full trading days
            # (accounting for weekends/holidays)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=120)
            
            hist = stock.history(start=start_date, end=end_date)
            
            if hist.empty:
                print(f"  ⚠️  No data available for {ticker}")
                failed_tickers.append({'ticker': ticker, 'company': company, 'reason': 'No data'})
                continue
            
            # Get today's opening price (most recent)
            today_open = hist['Open'].iloc[-1]
            
            # Calculate daily change (today's open vs yesterday's close)
            if len(hist) >= 2:
                yesterday_close = hist['Close'].iloc[-2]
                daily_change = ((today_open - yesterday_close) / yesterday_close) * 100
            else:
                daily_change = 0
                print(f"  ⚠️  Not enough data for daily change calculation for {ticker}")
            
            # Get price from 7 trading days ago (or closest available) for the 7-day change
            if len(hist) >= 7:
                week_ago_close = hist['Close'].iloc[-7]
            else:
                # If we don't have 7 days, use the oldest available
                week_ago_close = hist['Close'].iloc[0]
                print(f"  ⚠️  Only {len(hist)} days of history available for {ticker}")
            
            # Calculate 7-day percentage change
            seven_day_change = ((today_open - week_ago_close) / week_ago_close) * 100
            
            # Store last 30 days of closing prices and trading volume for charting
            recent_history = hist.tail(30)
            price_history = recent_history['Close'].tolist()
            volume_history = recent_history['Volume'].tolist()
            date_history = recent_history.index.strftime('%Y-%m-%d').tolist()
            
            # Calculate average volume for display
            avg_volume = sum(volume_history) / len(volume_history) if volume_history else 0
            vol_str = f"{avg_volume/1000000:.1f}M" if avg_volume >= 1000000 else f"{avg_volume/1000:.1f}K"
            
            results.append({
                'ticker': ticker,
                'company': company,
                'opening_price': round(today_open, 2),
                'daily_change_pct': round(daily_change, 2),
                'seven_day_change_pct': round(seven_day_change, 2),
                'price_history': '|'.join(map(str, price_history)),
                'date_history': '|'.join(date_history),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'volume_history': '|'.join(map(str, [int(v) for v in volume_history]))
            })
            
            print(f"  ✓ {company}: ${today_open:.2f} (Daily: {daily_change:+.2f}%, 7-day: {seven_day_change:+.2f}%) - {len(price_history)} days, Avg Vol: {vol_str}")
            
        except Exception as e:
            print(f"  ✗ Error fetching {ticker} ({company}): {str(e)}")
            failed_tickers.append({'ticker': ticker, 'company': company, 'reason': str(e)})
    
    # Create DataFrame
    stock_df = pd.DataFrame(results)
    
    # Save results with updated naming convention: YYYY-MM-DD-stock-data.csv
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    output_file = OUTPUT_DIR / f'{today}-stock-data.csv'
    
    stock_df.to_csv(output_file, index=False)
    print(f"\n✓ Stock data saved to {output_file}")
    print(f"  Successfully fetched: {len(results)} tickers")
    print(f"  Failed: {len(failed_tickers)} tickers")
    
    # Save failed tickers for debugging
    if failed_tickers:
        failed_df = pd.DataFrame(failed_tickers)
        failed_file = OUTPUT_DIR / f'{today}-failed-tickers.csv'
        failed_df.to_csv(failed_file, index=False)
        print(f"  Failed tickers saved to {failed_file}")
    
    return stock_df

if __name__ == "__main__":
    fetch_stock_data()
