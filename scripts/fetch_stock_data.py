#!/usr/bin/env python3
"""
Fetch stock data for companies in the roster and save to Cloud Storage
Updated to write to Google Cloud Storage
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import argparse
import sys
from pathlib import Path

# Add parent directory to path to import storage_utils
sys.path.append(str(Path(__file__).parent.parent))
from storage_utils import CloudStorageManager


def load_roster(storage=None, roster_path='rosters/main-roster.csv'):
    """
    Load company roster from Cloud Storage or local file
    
    Args:
        storage: CloudStorageManager instance (optional)
        roster_path: Path to roster file
    
    Returns:
        DataFrame with company information
    """
    try:
        if storage and storage.file_exists(roster_path):
            print(f"📋 Loading roster from Cloud Storage: {roster_path}")
            df = storage.read_csv(roster_path)
        else:
            print(f"📋 Loading roster from local file: {roster_path}")
            df = pd.read_csv(roster_path, encoding='utf-8-sig')
        
        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]
        
        # Clean up data
        df['ticker'] = df['ticker'].astype(str).str.strip()
        df['company'] = df['company'].astype(str).str.strip()
        
        # Filter out invalid rows
        df = df[(df['ticker'] != '') & (df['ticker'] != 'nan') & 
                (df['company'] != '') & (df['company'] != 'nan')]
        
        print(f"✅ Loaded {len(df)} companies from roster")
        return df
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        return pd.DataFrame()


def fetch_stock_data_for_company(ticker, company, days_back=30):
    """
    Fetch stock data for a single company
    
    Args:
        ticker: Stock ticker symbol
        company: Company name
        days_back: Number of days of historical data to fetch
    
    Returns:
        dict with stock data or None if failed
    """
    try:
        stock = yf.Ticker(ticker)
        
        # Get historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back + 10)  # Extra buffer
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"  ⚠️  {company} ({ticker}): No data available")
            return None
        
        # Get most recent data
        latest = hist.iloc[-1]
        previous = hist.iloc[-2] if len(hist) > 1 else latest
        
        # Calculate daily change (overnight gap: yesterday's close to today's open)
        opening_price = latest['Open']
        previous_close = previous['Close']
        daily_change = ((opening_price - previous_close) / previous_close * 100) if previous_close > 0 else 0
        
        # Calculate 7-day change
        week_ago = hist.iloc[-8] if len(hist) >= 8 else hist.iloc[0]
        seven_day_change = ((latest['Close'] - week_ago['Close']) / week_ago['Close'] * 100) if week_ago['Close'] > 0 else 0
        
        # Get last 30 days of closing prices
        recent_hist = hist.tail(30)
        price_history = '|'.join([f"{price:.2f}" for price in recent_hist['Close']])
        date_history = '|'.join([date.strftime('%Y-%m-%d') for date in recent_hist.index])
        volume_history = '|'.join([f"{int(vol)}" for vol in recent_hist['Volume']])
        
        result = {
            'ticker': ticker,
            'company': company,
            'opening_price': round(opening_price, 2),
            'daily_change': round(daily_change, 2),
            'seven_day_change': round(seven_day_change, 2),
            'price_history': price_history,
            'date_history': date_history,
            'volume_history': volume_history,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"  ✅ {company} ({ticker}): ${opening_price:.2f} ({daily_change:+.2f}%)")
        return result
        
    except Exception as e:
        print(f"  ❌ {company} ({ticker}): {str(e)}")
        return None


def fetch_all_stock_data(storage=None, roster_path='rosters/main-roster.csv', output_dir='data/stock_prices'):
    """
    Fetch stock data for all companies and save to Cloud Storage or local file
    
    Args:
        storage: CloudStorageManager instance (optional)
        roster_path: Path to roster CSV
        output_dir: Directory to save stock data
    """
    print("\n" + "="*60)
    print("📊 FETCHING STOCK DATA")
    print("="*60 + "\n")
    
    # Load roster
    roster_df = load_roster(storage, roster_path)
    
    if roster_df.empty:
        print("❌ No companies in roster, exiting")
        return
    
    # Fetch stock data for each company
    results = []
    total = len(roster_df)
    
    for idx, row in roster_df.iterrows():
        ticker = row['ticker']
        company = row['company']
        
        print(f"[{idx+1}/{total}] {company} ({ticker})")
        
        stock_data = fetch_stock_data_for_company(ticker, company, days_back=30)
        
        if stock_data:
            results.append(stock_data)
    
    # Create DataFrame
    if not results:
        print("\n❌ No stock data collected")
        return
    
    results_df = pd.DataFrame(results)
    
    # Save to Cloud Storage or local file
    today = datetime.now().strftime('%Y-%m-%d')
    output_path = f"{output_dir}/{today}-stock-data.csv"
    
    try:
        if storage:
            # Write to Cloud Storage
            storage.write_csv(results_df, output_path, index=False)
            print(f"\n✅ Saved to Cloud Storage: gs://{storage.bucket_name}/{output_path}")
            
            # Get public URL
            public_url = storage.get_public_url(output_path)
            print(f"🌐 Public URL: {public_url}")
        else:
            # Write to local file
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            results_df.to_csv(output_path, index=False)
            print(f"\n✅ Saved locally: {output_path}")
        
        # Print summary
        print(f"\n📊 Summary:")
        print(f"   Total companies: {len(roster_df)}")
        print(f"   Successful: {len(results)}")
        print(f"   Failed: {len(roster_df) - len(results)}")
        
        # Print some statistics
        avg_change = results_df['daily_change'].mean()
        positive = (results_df['daily_change'] > 0).sum()
        negative = (results_df['daily_change'] < 0).sum()
        
        print(f"\n📈 Market Summary:")
        print(f"   Average daily change: {avg_change:+.2f}%")
        print(f"   Positive: {positive} ({positive/len(results)*100:.1f}%)")
        print(f"   Negative: {negative} ({negative/len(results)*100:.1f}%)")
        
    except Exception as e:
        print(f"\n❌ Error saving data: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch stock data and save to Cloud Storage or local file'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        default='risk-dashboard',
        help='GCS bucket name (default: risk-dashboard)'
    )
    parser.add_argument(
        '--local',
        action='store_true',
        help='Use local file storage instead of GCS'
    )
    parser.add_argument(
        '--roster',
        type=str,
        default='rosters/main-roster.csv',
        help='Path to roster file (default: rosters/main-roster.csv)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/stock_prices',
        help='Output directory path (default: data/stock_prices)'
    )
    
    args = parser.parse_args()
    
    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    # Fetch and save stock data
    fetch_all_stock_data(
        storage=storage,
        roster_path=args.roster,
        output_dir=args.output_dir
    )


if __name__ == '__main__':
    main()
