#!/usr/bin/env python3
"""
Fetch stock data for companies in the roster and save to Cloud Storage
Uses batch downloading to avoid rate limiting
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import argparse
import sys
import time
from pathlib import Path

from storage_utils import CloudStorageManager
from db_writer import upsert_stock_df


def load_roster(storage=None, roster_path='rosters/main-roster.csv'):
    """Load company roster from Cloud Storage or local file"""
    try:
        if storage and storage.file_exists(roster_path):
            print(f"📋 Loading roster from Cloud Storage: {roster_path}")
            df = storage.read_csv(roster_path)
        else:
            print(f"📋 Loading roster from local file: {roster_path}")
            df = pd.read_csv(roster_path, encoding='utf-8-sig')
        
        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]
        print(f"  📋 Columns found: {list(df.columns)}")
        
        # Handle column name variations
        if 'ticker' in df.columns and 'stock' not in df.columns:
            df = df.rename(columns={'ticker': 'stock'})
            print("  ℹ️  Renamed 'ticker' column to 'stock'")
        
        # Clean up data
        df['stock'] = df['stock'].astype(str).str.strip()
        df['company'] = df['company'].astype(str).str.strip()
        
        # Filter out invalid rows
        df = df[(df['stock'] != '') & (df['stock'] != 'nan') & 
                (df['company'] != '') & (df['company'] != 'nan')]
        
        print(f"✅ Loaded {len(df)} companies from roster")
        return df
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        return pd.DataFrame()


def fetch_batch_with_retry(tickers, days_back=30, max_retries=3):
    """
    Fetch historical data for a batch of tickers with retry logic.
    Uses yfinance's batch download which is more efficient and less prone to rate limiting.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back + 10)
    
    for attempt in range(max_retries):
        try:
            # Batch download - much more efficient than individual requests
            data = yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                group_by='ticker',
                progress=False,
                threads=True,
                ignore_tz=True
            )
            return data
        except Exception as e:
            wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
            print(f"  ⚠️  Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"  ⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
    
    return None


def process_ticker_data(ticker, company, hist_data):
    """Process historical data for a single ticker into our format"""
    try:
        # Handle both single-ticker and multi-ticker DataFrames
        if isinstance(hist_data.columns, pd.MultiIndex):
            # Multi-ticker: data is grouped by ticker
            if ticker not in hist_data.columns.get_level_values(0):
                return None
            ticker_data = hist_data[ticker].dropna(how='all')
        else:
            # Single ticker: columns are just OHLCV
            ticker_data = hist_data.dropna(how='all')
        
        if ticker_data.empty or len(ticker_data) < 2:
            return None
        
        # Get most recent data
        latest = ticker_data.iloc[-1]
        previous = ticker_data.iloc[-2]
        
        # Calculate daily change (overnight gap)
        opening_price = latest['Open']
        previous_close = previous['Close']
        
        if pd.isna(opening_price) or pd.isna(previous_close) or previous_close == 0:
            return None
            
        daily_change = ((opening_price - previous_close) / previous_close * 100)
        
        # Calculate 7-day change
        week_ago = ticker_data.iloc[-8] if len(ticker_data) >= 8 else ticker_data.iloc[0]
        seven_day_change = ((latest['Close'] - week_ago['Close']) / week_ago['Close'] * 100) if week_ago['Close'] > 0 else 0
        
        # Get last 30 days of data
        recent = ticker_data.tail(30)
        price_history = '|'.join([f"{p:.2f}" for p in recent['Close'] if not pd.isna(p)])
        date_history = '|'.join([d.strftime('%Y-%m-%d') for d in recent.index])
        volume_history = '|'.join([f"{int(v)}" for v in recent['Volume'] if not pd.isna(v)])
        
        return {
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
        
    except Exception as e:
        print(f"  ⚠️  Error processing {ticker}: {e}")
        return None


def fetch_all_stock_data(storage=None, roster_path='rosters/main-roster.csv', 
                         output_dir='data/stock_prices', batch_size=100):
    """Fetch stock data for all companies using batch downloads"""
    print("\n" + "="*60)
    print("📊 FETCHING STOCK DATA (Batch Mode)")
    print("="*60 + "\n")
    
    # Load roster
    roster_df = load_roster(storage, roster_path)
    if roster_df.empty:
        print("❌ No companies in roster, exiting")
        return
    
    # Create ticker -> company mapping
    ticker_to_company = dict(zip(roster_df['stock'], roster_df['company']))
    all_tickers = list(ticker_to_company.keys())
    
    results = []
    failed_tickers = []
    
    # Process in batches
    num_batches = (len(all_tickers) + batch_size - 1) // batch_size
    
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(all_tickers))
        batch_tickers = all_tickers[start_idx:end_idx]
        
        print(f"\n📦 Batch {batch_num + 1}/{num_batches} ({len(batch_tickers)} tickers)")
        
        # Fetch batch data
        batch_data = fetch_batch_with_retry(batch_tickers)
        
        if batch_data is None or batch_data.empty:
            print(f"  ❌ Batch {batch_num + 1} failed completely")
            failed_tickers.extend(batch_tickers)
            continue
        
        # Process each ticker in the batch
        batch_success = 0
        for ticker in batch_tickers:
            company = ticker_to_company[ticker]
            result = process_ticker_data(ticker, company, batch_data)
            
            if result:
                results.append(result)
                batch_success += 1
            else:
                failed_tickers.append(ticker)
        
        print(f"  ✅ Batch {batch_num + 1}: {batch_success}/{len(batch_tickers)} successful")
        
        # Small delay between batches to avoid rate limiting
        if batch_num < num_batches - 1:
            time.sleep(2)
    
    # Create DataFrame and save
    if not results:
        print("\n❌ No stock data collected")
        return
    
    results_df = pd.DataFrame(results)
    today = datetime.now().strftime('%Y-%m-%d')
    output_path = f"{output_dir}/{today}-stock-data.csv"
    
    try:
        if storage:
            storage.write_csv(results_df, output_path, index=False)
            print(f"\n✅ Saved to Cloud Storage: gs://{storage.bucket_name}/{output_path}")
            public_url = storage.get_public_url(output_path)
            print(f"🌐 Public URL: {public_url}")
        else:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            results_df.to_csv(output_path, index=False)
            print(f"\n✅ Saved locally: {output_path}")
        
        # Print summary
        print(f"\n📊 Summary:")
        print(f"   Total companies: {len(roster_df)}")
        print(f"   Successful: {len(results)}")
        print(f"   Failed: {len(failed_tickers)}")
        
        if failed_tickers and len(failed_tickers) <= 20:
            print(f"   Failed tickers: {', '.join(failed_tickers)}")
        
        # Market stats
        avg_change = results_df['daily_change'].mean()
        positive = (results_df['daily_change'] > 0).sum()
        negative = (results_df['daily_change'] < 0).sum()
        
        print(f"\n📈 Market Summary:")
        print(f"   Average daily change: {avg_change:+.2f}%")
        print(f"   Positive: {positive} ({positive/len(results)*100:.1f}%)")
        print(f"   Negative: {negative} ({negative/len(results)*100:.1f}%)")
        try:
            db_count = upsert_stock_df(results_df)
            print(f"✅ DB upserted {db_count} stock price rows")
        except Exception as e:
            print(f"⚠️ DB upsert failed: {e}")
        
    except Exception as e:
        print(f"\n❌ Error saving data: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch stock data and save to Cloud Storage or local file'
    )
    parser.add_argument('--bucket', type=str, default='risk-dashboard',
                        help='GCS bucket name (default: risk-dashboard)')
    parser.add_argument('--local', action='store_true',
                        help='Use local file storage instead of GCS')
    parser.add_argument('--roster', type=str, default='rosters/main-roster.csv',
                        help='Path to roster file')
    parser.add_argument('--output-dir', type=str, default='data/stock_prices',
                        help='Output directory path')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of tickers to fetch per batch (default: 100)')
    
    args = parser.parse_args()
    
    storage = None
    if args.local:
        print("📁 Using local file storage")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    fetch_all_stock_data(
        storage=storage,
        roster_path=args.roster,
        output_dir=args.output_dir,
        batch_size=args.batch_size
    )


if __name__ == '__main__':
    main()
