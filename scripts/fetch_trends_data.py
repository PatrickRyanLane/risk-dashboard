"""
Fetch Google Trends search volume data for companies in the roster.
Retrieves relative search interest over the past 30 days to align with stock data.
Supports batched execution to avoid rate limits.

Usage:
  # Run specific batch (1-indexed)
  python fetch_trends_data.py --batch 1 --batch-size 300
  
  # Run all companies (no batching)
  python fetch_trends_data.py
  
  # Merge batch files into final output
  python fetch_trends_data.py --merge
"""

from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime, timedelta
import time
from pathlib import Path
import random
import argparse

# Suppress the pandas FutureWarning from pytrends
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)

# Set up paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
ROSTER_PATH = PROJECT_ROOT / 'rosters' / 'main-roster.csv'
STOCK_DATA_DIR = PROJECT_ROOT / 'data' / 'stock_prices'
OUTPUT_DIR = PROJECT_ROOT / 'data' / 'trends_data'

# Rate limit settings
MIN_DELAY = 2.0
MAX_DELAY = 4.0
RETRY_DELAY = 10.0
MAX_RETRIES = 2

# Batch settings
DEFAULT_BATCH_SIZE = 300

def merge_batch_files():
    """
    Merge all batch files from today into a single trends data file.
    """
    today = datetime.now().strftime('%Y-%m-%d')
    batch_files = sorted(OUTPUT_DIR.glob(f'{today}-trends-data-batch*.csv'))
    
    if not batch_files:
        print(f"❌ No batch files found for {today}")
        return None
    
    print(f"📦 Merging {len(batch_files)} batch files...")
    
    all_data = []
    for batch_file in batch_files:
        df = pd.read_csv(batch_file)
        all_data.append(df)
        print(f"  ✓ Loaded {batch_file.name}: {len(df)} companies")
    
    # Combine all batches
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates (keep first occurrence)
    merged_df = merged_df.drop_duplicates(subset=['company'], keep='first')
    
    # Save merged file
    output_file = OUTPUT_DIR / f'{today}-trends-data.csv'
    merged_df.to_csv(output_file, index=False)
    
    print(f"\n✓ Merged data saved to {output_file}")
    print(f"  Total companies: {len(merged_df)}")
    print(f"  Successfully fetched: {len(merged_df[merged_df['trends_history'] != ''])}")
    
    return merged_df

def fetch_trends_data(batch_num=None, batch_size=DEFAULT_BATCH_SIZE):
    """
    Fetch Google Trends data for companies.
    
    Args:
        batch_num: Batch number (1-indexed). If None, fetch all companies.
        batch_size: Number of companies per batch.
    """
    print("Loading roster...")
    roster_df = pd.read_csv(ROSTER_PATH)
    
    # Load the most recent stock data
    stock_files = sorted(STOCK_DATA_DIR.glob('*-stock-data.csv'))
    if not stock_files:
        print("❌ No stock data files found. Please run fetch_stock_data.py first.")
        return None
    
    latest_stock_file = stock_files[-1]
    print(f"Using dates from: {latest_stock_file.name}")
    stock_df = pd.read_csv(latest_stock_file)
    
    # Get unique companies from stock data
    all_companies = stock_df['company'].unique().tolist()
    total_companies = len(all_companies)
    
    # Calculate batch range
    if batch_num is not None:
        start_idx = (batch_num - 1) * batch_size
        end_idx = min(start_idx + batch_size, total_companies)
        companies = all_companies[start_idx:end_idx]
        
        print(f"\n📊 BATCH {batch_num}")
        print(f"   Processing companies {start_idx + 1}-{end_idx} of {total_companies}")
        print(f"   ({len(companies)} companies in this batch)")
    else:
        companies = all_companies
        print(f"Found {len(companies)} companies to fetch trends for (no batching)")
    
    # Initialize pytrends
    pytrends = TrendReq(hl='en-US', tz=360)
    
    results = []
    failed_companies = []
    consecutive_rate_limits = 0
    MAX_CONSECUTIVE_RATE_LIMITS = 5
    
    for idx, company in enumerate(companies):
        # Early exit if we're clearly rate limited
        if consecutive_rate_limits >= MAX_CONSECUTIVE_RATE_LIMITS:
            print(f"\n⚠️  Hit {MAX_CONSECUTIVE_RATE_LIMITS} consecutive rate limits. Stopping to avoid further blocking.")
            print(f"   Collected data for {len([r for r in results if r['trends_history']])} companies.")
            
            # Add empty results for remaining companies in this batch
            for remaining_company in companies[idx:]:
                stock_row = stock_df[stock_df['company'] == remaining_company].iloc[0]
                results.append({
                    'company': remaining_company,
                    'trends_history': '',
                    'date_history': stock_row['date_history'],
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'avg_interest': 0
                })
            break
        
        retry_count = 0
        success = False
        
        while retry_count <= MAX_RETRIES and not success:
            try:
                if retry_count > 0:
                    print(f"  🔄 Retry {retry_count}/{MAX_RETRIES} for {company}...")
                else:
                    progress = f"[{idx + 1}/{len(companies)}]"
                    print(f"{progress} Fetching trends for {company}...")
                
                # Get the date range from the stock data
                stock_row = stock_df[stock_df['company'] == company].iloc[0]
                date_history = stock_row['date_history'].split('|')
                
                # Use the date range from stock data
                start_date = date_history[0]
                end_date = date_history[-1]
                timeframe = f"{start_date} {end_date}"
                
                search_terms = [company]
                
                # Re-initialize pytrends for each request
                pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))
                pytrends.build_payload(search_terms, timeframe=timeframe, geo='US')
                
                # Get interest over time
                interest_df = pytrends.interest_over_time()
                
                if interest_df.empty or company not in interest_df.columns:
                    print(f"  ⚠️  No trends data available for {company}")
                    failed_companies.append({'company': company, 'reason': 'No data'})
                    
                    results.append({
                        'company': company,
                        'trends_history': '',
                        'date_history': stock_row['date_history'],
                        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'avg_interest': 0
                    })
                    success = True
                    continue
                
                # Get the search interest values
                interest_values = interest_df[company].tolist()
                trends_dates = interest_df.index.strftime('%Y-%m-%d').tolist()
                trends_map = dict(zip(trends_dates, interest_values))
                
                # Align with stock dates
                aligned_trends = []
                for stock_date in date_history:
                    if stock_date in trends_map:
                        aligned_trends.append(trends_map[stock_date])
                    else:
                        earlier_values = [v for d, v in trends_map.items() if d <= stock_date]
                        if earlier_values:
                            aligned_trends.append(earlier_values[-1])
                        else:
                            aligned_trends.append(list(trends_map.values())[0] if trends_map else 0)
                
                avg_interest = sum(aligned_trends) / len(aligned_trends) if aligned_trends else 0
                
                results.append({
                    'company': company,
                    'trends_history': '|'.join(map(str, [int(v) for v in aligned_trends])),
                    'date_history': stock_row['date_history'],
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'avg_interest': round(avg_interest, 1)
                })
                
                print(f"  ✓ {company}: Avg interest: {avg_interest:.1f}/100")
                
                success = True
                consecutive_rate_limits = 0
                
                # Add delay
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                time.sleep(delay)
                
            except Exception as e:
                error_msg = str(e)
                
                if '429' in error_msg or 'rate limit' in error_msg.lower():
                    retry_count += 1
                    consecutive_rate_limits += 1
                    
                    if retry_count <= MAX_RETRIES:
                        backoff_delay = RETRY_DELAY * (2 ** (retry_count - 1))
                        print(f"  ⚠️  Rate limited (429). Waiting {backoff_delay:.0f}s before retry...")
                        time.sleep(backoff_delay)
                    else:
                        print(f"  ✗ Rate limit exceeded for {company}")
                        failed_companies.append({'company': company, 'reason': 'Rate limit (429)'})
                        
                        stock_row = stock_df[stock_df['company'] == company].iloc[0]
                        results.append({
                            'company': company,
                            'trends_history': '',
                            'date_history': stock_row['date_history'],
                            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'avg_interest': 0
                        })
                        time.sleep(RETRY_DELAY)
                else:
                    print(f"  ✗ Error fetching trends for {company}: {error_msg}")
                    failed_companies.append({'company': company, 'reason': error_msg})
                    
                    stock_row = stock_df[stock_df['company'] == company].iloc[0]
                    results.append({
                        'company': company,
                        'trends_history': '',
                        'date_history': stock_row['date_history'],
                        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'avg_interest': 0
                    })
                    
                    success = True
                    consecutive_rate_limits = 0
                    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    
    # Create DataFrame
    trends_df = pd.DataFrame(results)
    
    # Save results
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    
    if batch_num is not None:
        output_file = OUTPUT_DIR / f'{today}-trends-data-batch{batch_num}.csv'
    else:
        output_file = OUTPUT_DIR / f'{today}-trends-data.csv'
    
    trends_df.to_csv(output_file, index=False)
    
    # Calculate statistics
    total = len(results)
    successful = len([r for r in results if r['trends_history']])
    rate_limited = len([f for f in failed_companies if '429' in str(f.get('reason', ''))])
    
    print(f"\n✓ Trends data saved to {output_file}")
    print(f"  Total companies: {total}")
    print(f"  Successfully fetched: {successful}")
    print(f"  Rate limited (429): {rate_limited}")
    
    if batch_num is not None:
        print(f"\n💡 Run with --merge after all batches complete to create final file")
    
    # Save failed companies
    if failed_companies:
        failed_df = pd.DataFrame(failed_companies)
        if batch_num is not None:
            failed_file = OUTPUT_DIR / f'{today}-failed-trends-batch{batch_num}.csv'
        else:
            failed_file = OUTPUT_DIR / f'{today}-failed-trends.csv'
        failed_df.to_csv(failed_file, index=False)
        print(f"  Failed companies saved to {failed_file}")
    
    return trends_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Google Trends data for companies')
    parser.add_argument('--batch', type=int, help='Batch number (1-indexed)')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE, 
                        help=f'Number of companies per batch (default: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--merge', action='store_true', 
                        help='Merge batch files into final output')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Google Trends Data Fetcher (Batched)")
    print("=" * 60)
    
    if args.merge:
        merge_batch_files()
    else:
        if args.batch:
            print(f"\n📦 Running BATCH {args.batch} (size: {args.batch_size})")
        else:
            print("\n⚠️  Running without batching (all companies)")
        
        print("\n⚠️  NOTE: Google Trends has rate limits.")
        print("Expect ~10-20 minutes per 300-company batch.\n")
        
        fetch_trends_data(batch_num=args.batch, batch_size=args.batch_size)
