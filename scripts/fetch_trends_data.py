#!/usr/bin/env python3
"""
Fetch Google Trends data for companies in the roster and save to Cloud Storage
Updated to write to Google Cloud Storage
"""

from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime, timedelta
import time
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
        df['company'] = df['company'].astype(str).str.strip()
        
        # Filter out invalid rows
        df = df[(df['company'] != '') & (df['company'] != 'nan')]
        
        print(f"✅ Loaded {len(df)} companies from roster")
        return df
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        return pd.DataFrame()


def fetch_trends_for_company(company_name, pytrends, days_back=30):
    """
    Fetch Google Trends data for a single company
    
    Args:
        company_name: Name of the company
        pytrends: TrendReq instance
        days_back: Number of days of historical data
    
    Returns:
        dict with trends data or None if failed
    """
    try:
        # Build payload
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        
        pytrends.build_payload(
            kw_list=[company_name],
            timeframe=timeframe,
            geo='US'
        )
        
        # Get interest over time
        trends_df = pytrends.interest_over_time()
        
        if trends_df.empty or company_name not in trends_df.columns:
            print(f"  ⚠️  {company_name}: No trends data available")
            return None
        
        # Extract data
        trends_values = trends_df[company_name].values
        trends_dates = [date.strftime('%Y-%m-%d') for date in trends_df.index]
        
        # Calculate statistics
        avg_interest = trends_values.mean()
        max_interest = trends_values.max()
        current_interest = trends_values[-1] if len(trends_values) > 0 else 0
        
        result = {
            'company': company_name,
            'trends_history': '|'.join([str(int(val)) for val in trends_values]),
            'date_history': '|'.join(trends_dates),
            'average_interest': round(avg_interest, 2),
            'max_interest': int(max_interest),
            'current_interest': int(current_interest),
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        print(f"  ✅ {company_name}: Current={current_interest}, Avg={avg_interest:.1f}, Max={max_interest}")
        return result
        
    except Exception as e:
        print(f"  ❌ {company_name}: {str(e)}")
        return None


def fetch_all_trends_data(storage=None, roster_path='rosters/main-roster.csv', 
                          output_dir='data/trends_data', batch_size=5):
    """
    Fetch Google Trends data for all companies and save to Cloud Storage or local file
    
    Args:
        storage: CloudStorageManager instance (optional)
        roster_path: Path to roster CSV
        output_dir: Directory to save trends data
        batch_size: Number of companies to process before pausing (rate limiting)
    """
    print("\n" + "="*60)
    print("🔍 FETCHING GOOGLE TRENDS DATA")
    print("="*60 + "\n")
    
    # Load roster
    roster_df = load_roster(storage, roster_path)
    
    if roster_df.empty:
        print("❌ No companies in roster, exiting")
        return
    
    # Initialize pytrends
    pytrends = TrendReq(hl='en-US', tz=360)
    
    # Fetch trends data for each company
    results = []
    total = len(roster_df)
    
    for idx, row in roster_df.iterrows():
        company = row['company']
        
        print(f"[{idx+1}/{total}] {company}")
        
        trends_data = fetch_trends_for_company(company, pytrends, days_back=30)
        
        if trends_data:
            results.append(trends_data)
        
        # Rate limiting: pause every batch_size requests
        if (idx + 1) % batch_size == 0 and idx + 1 < total:
            print(f"\n⏸️  Pausing for 30 seconds (rate limiting)...\n")
            time.sleep(30)
        else:
            # Small delay between requests
            time.sleep(2)
    
    # Create DataFrame
    if not results:
        print("\n❌ No trends data collected")
        return
    
    results_df = pd.DataFrame(results)
    
    # Save to Cloud Storage or local file
    today = datetime.now().strftime('%Y-%m-%d')
    output_path = f"{output_dir}/{today}-trends-data.csv"
    
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
        avg_interest = results_df['average_interest'].mean()
        high_interest = (results_df['current_interest'] >= 50).sum()
        
        print(f"\n🔍 Trends Summary:")
        print(f"   Average interest: {avg_interest:.1f}")
        print(f"   High interest (≥50): {high_interest}")
        
    except Exception as e:
        print(f"\n❌ Error saving data: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch Google Trends data and save to Cloud Storage or local file'
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
        default='data/trends_data',
        help='Output directory path (default: data/trends_data)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=5,
        help='Number of companies to process before pausing (default: 5)'
    )
    
    args = parser.parse_args()
    
    # Initialize storage (GCS by default, local with --local flag)
    storage = None
    if args.local:
        print("📁 Using local file storage (--local flag)")
    else:
        print(f"☁️  Using Cloud Storage bucket: {args.bucket}")
        storage = CloudStorageManager(args.bucket)
    
    # Fetch and save trends data
    fetch_all_trends_data(
        storage=storage,
        roster_path=args.roster,
        output_dir=args.output_dir,
        batch_size=args.batch_size
    )


if __name__ == '__main__':
    main()
