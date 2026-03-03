#!/usr/bin/env python3
"""
Fetch Google Trends data for companies in the roster and save to Cloud Storage.
Supports batched execution and a merge mode used by the GitHub workflow.
"""

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pytrends.request import TrendReq

from db_writer import upsert_trends_df
from storage_utils import CloudStorageManager


def load_roster(storage=None, roster_path='rosters/main-roster.csv'):
    """
    Load company roster from Cloud Storage or local file.
    """
    try:
        if storage and storage.file_exists(roster_path):
            print(f"📋 Loading roster from Cloud Storage: {roster_path}")
            df = storage.read_csv(roster_path)
        else:
            print(f"📋 Loading roster from local file: {roster_path}")
            df = pd.read_csv(roster_path, encoding='utf-8-sig')

        df.columns = [c.strip().lower() for c in df.columns]
        df['company'] = df['company'].astype(str).str.strip()
        df = df[(df['company'] != '') & (df['company'] != 'nan')]

        print(f"✅ Loaded {len(df)} companies from roster")
        return df
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        return pd.DataFrame()


def fetch_trends_for_company(company_name, pytrends, days_back=30):
    """
    Fetch Google Trends data for a single company.
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

        pytrends.build_payload(kw_list=[company_name], timeframe=timeframe, geo='US')
        trends_df = pytrends.interest_over_time()

        if trends_df.empty or company_name not in trends_df.columns:
            print(f"  ⚠️  {company_name}: No trends data available")
            return None

        trends_values = trends_df[company_name].values
        trends_dates = [date.strftime('%Y-%m-%d') for date in trends_df.index]

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
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        print(f"  ✅ {company_name}: Current={current_interest}, Avg={avg_interest:.1f}, Max={max_interest}")
        return result
    except Exception as e:
        print(f"  ❌ {company_name}: {str(e)}")
        return None


def _split_roster_for_batch(roster_df: pd.DataFrame, batch: int, total_batches: int) -> pd.DataFrame:
    """
    Deterministically split roster rows into N equal-ish batches and return one slice.
    """
    if batch < 1 or batch > total_batches:
        raise ValueError(f"batch must be between 1 and {total_batches} (got: {batch})")
    total = len(roster_df)
    if total == 0:
        return roster_df
    start = ((batch - 1) * total) // total_batches
    end = (batch * total) // total_batches
    return roster_df.iloc[start:end].reset_index(drop=True)


def fetch_all_trends_data(
    storage=None,
    roster_path='rosters/main-roster.csv',
    output_dir='data/trends_data',
    batch_size=5,
    batch=None,
    total_batches=3,
    upsert_db=True,
):
    """
    Fetch Google Trends data and write batch/final outputs.

    Args:
        storage: CloudStorageManager instance (optional)
        roster_path: Path to roster CSV
        output_dir: Directory to save trends data
        batch_size: Number of companies to process before pausing (rate limiting)
        batch: Optional batch number to process (1-indexed)
        total_batches: Total batches used when splitting roster
        upsert_db: Whether to upsert to DB after writing output
    """
    print("\n" + "=" * 60)
    print("🔍 FETCHING GOOGLE TRENDS DATA")
    print("=" * 60 + "\n")

    roster_df = load_roster(storage, roster_path)
    if roster_df.empty:
        print("❌ No companies in roster, exiting")
        return pd.DataFrame()

    source_count = len(roster_df)
    if batch is not None:
        roster_df = _split_roster_for_batch(roster_df, batch, total_batches)
        print(f"📦 Batch {batch}/{total_batches}: {len(roster_df)} of {source_count} companies")
        if roster_df.empty:
            print("⚠️ Batch slice is empty; nothing to fetch")
            return pd.DataFrame()

    pytrends = TrendReq(hl='en-US', tz=360)

    results = []
    total = len(roster_df)

    for idx, row in roster_df.iterrows():
        company = row['company']
        print(f"[{idx + 1}/{total}] {company}")

        trends_data = fetch_trends_for_company(company, pytrends, days_back=30)
        if trends_data:
            results.append(trends_data)

        if (idx + 1) % batch_size == 0 and idx + 1 < total:
            print("\n⏸️  Pausing for 30 seconds (rate limiting)...\n")
            time.sleep(30)
        else:
            time.sleep(2)

    if not results:
        print("\n❌ No trends data collected")
        return pd.DataFrame()

    results_df = pd.DataFrame(results)

    today = datetime.now().strftime('%Y-%m-%d')
    if batch is not None:
        output_path = f"{output_dir}/{today}-trends-data-batch{batch}.csv"
    else:
        output_path = f"{output_dir}/{today}-trends-data.csv"

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

        print("\n📊 Summary:")
        print(f"   Source companies: {source_count}")
        print(f"   Processed in this run: {total}")
        print(f"   Successful: {len(results)}")
        print(f"   Failed: {total - len(results)}")

        avg_interest = results_df['average_interest'].mean()
        high_interest = (results_df['current_interest'] >= 50).sum()

        print("\n🔍 Trends Summary:")
        print(f"   Average interest: {avg_interest:.1f}")
        print(f"   High interest (≥50): {high_interest}")

        if upsert_db:
            try:
                db_count = upsert_trends_df(results_df)
                print(f"✅ DB upserted {db_count} trends rows")
            except Exception as e:
                print(f"⚠️ DB upsert failed: {e}")
        else:
            print("ℹ️ Skipping DB upsert for batch output (merge step will upsert combined file).")

    except Exception as e:
        print(f"\n❌ Error saving data: {e}")

    return results_df


def merge_batch_outputs(storage=None, output_dir='data/trends_data', total_batches=3):
    """
    Merge today's batch files into the canonical trends CSV and upsert DB.
    """
    today = datetime.now().strftime('%Y-%m-%d')
    batch_paths = [f"{output_dir}/{today}-trends-data-batch{i}.csv" for i in range(1, total_batches + 1)]

    parts = []
    present_paths = []
    for path in batch_paths:
        try:
            if storage:
                if not storage.file_exists(path):
                    continue
                df = storage.read_csv(path)
            else:
                local_path = Path(path)
                if not local_path.exists():
                    continue
                df = pd.read_csv(local_path)

            if df is None or df.empty:
                continue
            parts.append(df)
            present_paths.append(path)
        except Exception as e:
            print(f"⚠️ Failed to load batch file {path}: {e}")

    if not parts:
        print("❌ No batch files found to merge.")
        return pd.DataFrame()

    print(f"🔀 Merging {len(parts)} batch files...")
    merged = pd.concat(parts, ignore_index=True)
    deduped = 0
    if 'company' in merged.columns:
        before = len(merged)
        merged = merged.drop_duplicates(subset=['company'], keep='first')
        deduped = before - len(merged)

    output_path = f"{output_dir}/{today}-trends-data.csv"
    if storage:
        storage.write_csv(merged, output_path, index=False)
        print(f"✅ Wrote merged trends file: gs://{storage.bucket_name}/{output_path}")
    else:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        merged.to_csv(output_path, index=False)
        print(f"✅ Wrote merged trends file: {output_path}")

    print(f"📊 Merge summary: rows={len(merged)} deduped={deduped} files={len(present_paths)}")
    missing = [p for p in batch_paths if p not in present_paths]
    if missing:
        print(f"⚠️ Missing batch files ({len(missing)}):")
        for path in missing:
            print(f"   - {path}")

    try:
        db_count = upsert_trends_df(merged)
        print(f"✅ DB upserted {db_count} trends rows (merged output)")
    except Exception as e:
        print(f"⚠️ DB upsert failed after merge: {e}")

    return merged


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
    parser.add_argument(
        '--batch',
        type=int,
        default=None,
        help='Batch number to process (1-indexed)'
    )
    parser.add_argument(
        '--total-batches',
        type=int,
        default=3,
        help='Total number of batches used for splitting (default: 3)'
    )
    parser.add_argument(
        '--merge',
        action='store_true',
        help="Merge today's batch files into the final trends file and upsert DB"
    )

    args = parser.parse_args()

    if args.total_batches < 1:
        parser.error('--total-batches must be >= 1')
    if args.batch is not None and (args.batch < 1 or args.batch > args.total_batches):
        parser.error(f'--batch must be between 1 and {args.total_batches}')

    storage = None
    if args.local:
        print('📁 Using local file storage (--local flag)')
    else:
        print(f'☁️  Using Cloud Storage bucket: {args.bucket}')
        storage = CloudStorageManager(args.bucket)

    if args.merge:
        merge_batch_outputs(
            storage=storage,
            output_dir=args.output_dir,
            total_batches=args.total_batches,
        )
        return

    fetch_all_trends_data(
        storage=storage,
        roster_path=args.roster,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        batch=args.batch,
        total_batches=args.total_batches,
        upsert_db=(args.batch is None),
    )


if __name__ == '__main__':
    main()
