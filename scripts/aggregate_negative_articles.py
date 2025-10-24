#!/usr/bin/env python3
"""
Aggregate negative article data for stock chart heatmap visualization.

UPDATED: Handles brand articles without CEO column using smart company name matching.
Includes fuzzy matching for common company name variations.

Output: data/daily_counts/negative-articles-summary.csv
"""

import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
import re


def normalize_company_name(name):
    """
    Normalize company names for matching.
    Handles common variations like "Apple Inc." vs "Apple"
    """
    if not name or name == 'nan':
        return ''
    
    name = str(name).strip()
    
    # Remove common suffixes
    suffixes = [
        ' Inc.', ' Inc', ' Corporation', ' Corp.', ' Corp',
        ' Company', ' Co.', ' Co', ' LLC', ' L.L.C.', ' LP', ' L.P.',
        ' Ltd.', ' Ltd', ' Limited', ' PLC', ' plc',
        '.com', '.net', '.org'
    ]
    
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    
    # Remove special characters but keep spaces
    name = re.sub(r'[^\w\s]', '', name)
    
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name


def load_roster(roster_path='rosters/main-roster.csv'):
    """
    Load roster and create company -> CEO mapping with fuzzy matching support.
    
    Returns:
        tuple: (exact_mapping, normalized_mapping, all_companies)
    """
    try:
        df = pd.read_csv(roster_path, encoding='utf-8-sig')
        
        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]
        
        # Extract company and CEO columns
        if 'company' not in df.columns or 'ceo' not in df.columns:
            print(f"⚠️  Roster missing 'company' or 'ceo' columns")
            return {}, {}, set()
        
        # Clean up
        df['company'] = df['company'].astype(str).str.strip()
        df['ceo'] = df['ceo'].astype(str).str.strip()
        
        # Filter out invalid rows
        df = df[(df['company'] != '') & (df['company'] != 'nan') & 
                (df['ceo'] != '') & (df['ceo'] != 'nan')]
        
        # Create exact mapping
        exact_mapping = dict(zip(df['company'], df['ceo']))
        
        # Create normalized mapping for fuzzy matching
        normalized_mapping = {}
        for company, ceo in exact_mapping.items():
            normalized = normalize_company_name(company)
            if normalized:
                normalized_mapping[normalized.lower()] = ceo
        
        all_companies = set(df['company'].tolist())
        
        print(f"📋 Loaded roster: {len(exact_mapping)} company-CEO mappings")
        
        return exact_mapping, normalized_mapping, all_companies
        
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        return {}, {}, set()


def find_ceo_for_company(company, exact_mapping, normalized_mapping):
    """
    Find CEO for a company using exact then fuzzy matching.
    
    Returns:
        tuple: (ceo_name, match_type) or (None, None)
    """
    if not company or company == 'nan':
        return None, None
    
    # Try exact match first
    if company in exact_mapping:
        return exact_mapping[company], 'exact'
    
    # Try case-insensitive exact match
    for roster_company, ceo in exact_mapping.items():
        if roster_company.lower() == company.lower():
            return ceo, 'case-insensitive'
    
    # Try normalized match
    normalized = normalize_company_name(company).lower()
    if normalized in normalized_mapping:
        return normalized_mapping[normalized], 'normalized'
    
    # Try partial match (if company name contains roster name or vice versa)
    company_lower = company.lower()
    for roster_company, ceo in exact_mapping.items():
        roster_lower = roster_company.lower()
        # Check if one contains the other
        if company_lower in roster_lower or roster_lower in company_lower:
            # Make sure it's not a tiny substring match
            if len(company_lower) >= 3 and len(roster_lower) >= 3:
                return ceo, 'partial'
    
    return None, None


def process_ceo_articles(file_path):
    """Process CEO articles file (has CEO column)."""
    if not file_path.exists():
        return []
    
    try:
        df = pd.read_csv(file_path)
        
        if df.empty:
            return []
        
        # Normalize column names
        df.columns = [c.lower().strip() for c in df.columns]
        
        # Ensure required columns exist
        required_cols = ['ceo', 'company', 'sentiment', 'title']
        for col in required_cols:
            if col not in df.columns:
                return []
        
        # Clean up data
        df['sentiment'] = df['sentiment'].astype(str).str.lower().str.strip()
        df['ceo'] = df['ceo'].astype(str).str.strip()
        df['company'] = df['company'].astype(str).str.strip()
        df['title'] = df['title'].astype(str).str.strip()
        
        # Filter for negative sentiment only
        negative = df[df['sentiment'] == 'negative']
        
        if negative.empty:
            return []
        
        summary_data = []
        
        # Group by company/CEO and aggregate
        for (ceo, company), group in negative.groupby(['ceo', 'company']):
            if not ceo or not company or ceo == 'nan' or company == 'nan':
                continue
                
            count = len(group)
            
            # Get top 3 headlines
            headlines = []
            for title in group['title'].head(3):
                title_str = str(title).strip()
                if len(title_str) > 80:
                    title_str = title_str[:77] + '...'
                headlines.append(title_str)
            
            summary_data.append({
                'ceo': ceo,
                'company': company,
                'negative_count': count,
                'top_headlines': '|'.join(headlines),
                'article_type': 'ceo'
            })
        
        return summary_data
    
    except Exception as e:
        return []


def process_brand_articles(file_path, exact_mapping, normalized_mapping):
    """
    Process brand articles file (NO CEO column - look it up with fuzzy matching).
    """
    if not file_path.exists():
        return []
    
    try:
        df = pd.read_csv(file_path)
        
        if df.empty:
            return []
        
        # Normalize column names
        df.columns = [c.lower().strip() for c in df.columns]
        
        # Brand files have: company, title, url, source, date, sentiment
        required_cols = ['company', 'sentiment', 'title']
        for col in required_cols:
            if col not in df.columns:
                return []
        
        # Clean up data
        df['sentiment'] = df['sentiment'].astype(str).str.lower().str.strip()
        df['company'] = df['company'].astype(str).str.strip()
        df['title'] = df['title'].astype(str).str.strip()
        
        # Filter for negative sentiment only
        negative = df[df['sentiment'] == 'negative']
        
        if negative.empty:
            return []
        
        summary_data = []
        unmatched_companies = set()
        match_stats = {'exact': 0, 'case-insensitive': 0, 'normalized': 0, 'partial': 0, 'failed': 0}
        
        # Group by company
        for company, group in negative.groupby('company'):
            if not company or company == 'nan':
                continue
            
            # Look up CEO from roster with fuzzy matching
            ceo, match_type = find_ceo_for_company(company, exact_mapping, normalized_mapping)
            
            if not ceo:
                unmatched_companies.add(company)
                match_stats['failed'] += 1
                continue
            
            match_stats[match_type] += 1
                
            count = len(group)
            
            # Get top 3 headlines
            headlines = []
            for title in group['title'].head(3):
                title_str = str(title).strip()
                if len(title_str) > 80:
                    title_str = title_str[:77] + '...'
                headlines.append(title_str)
            
            summary_data.append({
                'ceo': ceo,
                'company': company,
                'negative_count': count,
                'top_headlines': '|'.join(headlines),
                'article_type': 'brand'
            })
        
        # Report matching stats
        if match_stats['failed'] > 0:
            print(f"  ⚠️  {match_stats['failed']} companies not matched to CEOs")
            if unmatched_companies and len(unmatched_companies) <= 10:
                print(f"     Unmatched: {', '.join(sorted(unmatched_companies))}")
        
        matched_total = sum(v for k, v in match_stats.items() if k != 'failed')
        if matched_total > 0:
            print(f"  ✓ {matched_total} companies matched successfully")
            if match_stats['normalized'] > 0 or match_stats['partial'] > 0:
                print(f"     (including {match_stats['normalized']} normalized, {match_stats['partial']} partial)")
        
        return summary_data
    
    except Exception as e:
        print(f"⚠️  Error processing {file_path.name}: {e}")
        return []


def create_negative_summary(days_back=90, roster_path='rosters/main-roster.csv'):
    """Create aggregated negative articles summary from last N days."""
    articles_dir = Path("data/processed_articles")
    output_file = Path("data/daily_counts/negative-articles-summary.csv")
    
    if not articles_dir.exists():
        print(f"❌ Articles directory not found: {articles_dir}")
        return
    
    # Load roster for company -> CEO mapping
    exact_mapping, normalized_mapping, all_companies = load_roster(roster_path)
    if not exact_mapping:
        print("⚠️  Warning: No roster loaded. Brand articles will be skipped.")
    
    all_summary_data = []
    today = datetime.now(timezone.utc)
    
    print(f"\n🔍 Scanning last {days_back} days for negative articles...")
    
    days_processed = 0
    ceo_files_found = 0
    brand_files_found = 0
    ceo_articles_count = 0
    brand_articles_count = 0
    
    for i in range(days_back):
        date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        
        # Process CEO articles (have CEO column)
        ceo_file = articles_dir / f"{date}-ceo-articles-modal.csv"
        if ceo_file.exists():
            ceo_files_found += 1
            ceo_data = process_ceo_articles(ceo_file)
            for item in ceo_data:
                item['date'] = date
                all_summary_data.append(item)
                ceo_articles_count += 1
        
        # Process brand articles (NO CEO column - look up from roster)
        brand_file = articles_dir / f"{date}-brand-articles-modal.csv"
        if brand_file.exists():
            brand_files_found += 1
            brand_data = process_brand_articles(brand_file, exact_mapping, normalized_mapping)
            for item in brand_data:
                item['date'] = date
                all_summary_data.append(item)
                brand_articles_count += 1
        
        if ceo_file.exists() or brand_file.exists():
            days_processed += 1
    
    print(f"\n📁 Files found: {ceo_files_found} CEO, {brand_files_found} brand ({days_processed} days with data)")
    print(f"📊 Article summaries created: {ceo_articles_count} CEO, {brand_articles_count} brand")
    
    # Create summary DataFrame
    if all_summary_data:
        summary_df = pd.DataFrame(all_summary_data)
        summary_df = summary_df.sort_values(['company', 'date', 'article_type'])
        summary_df = summary_df[['date', 'company', 'ceo', 'negative_count', 'top_headlines', 'article_type']]
        
        # Show helpful stats
        print(f"\n{'='*60}")
        print("📊 COMPANIES WITH BOTH CEO AND BRAND ARTICLES")
        print('='*60)
        
        companies_with_both = []
        for company in summary_df['company'].unique():
            company_data = summary_df[summary_df['company'] == company]
            types = company_data['article_type'].unique()
            if len(types) > 1:
                companies_with_both.append(company)
                ceo_count = company_data[company_data['article_type'] == 'ceo']['negative_count'].sum()
                brand_count = company_data[company_data['article_type'] == 'brand']['negative_count'].sum()
        
        if companies_with_both:
            print(f"✅ {len(companies_with_both)} companies have both CEO and brand negative articles!")
            # Show a few examples
            for company in sorted(companies_with_both)[:5]:
                company_data = summary_df[summary_df['company'] == company]
                ceo_count = company_data[company_data['article_type'] == 'ceo']['negative_count'].sum()
                brand_count = company_data[company_data['article_type'] == 'brand']['negative_count'].sum()
                print(f"  • {company}: {ceo_count} CEO, {brand_count} brand")
        else:
            print("⚠️  No companies have both types")
            
    else:
        print("⚠️  No negative articles found in the specified time range")
        summary_df = pd.DataFrame(columns=[
            'date', 'company', 'ceo', 'negative_count', 'top_headlines', 'article_type'
        ])
    
    # Write to CSV
    output_file.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(output_file, index=False)
    
    print(f"\n✅ Created {output_file}")
    print(f"📊 Total rows: {len(summary_df):,}")
    
    if not summary_df.empty:
        file_size_kb = output_file.stat().st_size / 1024
        print(f"📊 File size: {file_size_kb:.1f} KB")
        
        ceo_count = len(summary_df[summary_df['article_type'] == 'ceo'])
        brand_count = len(summary_df[summary_df['article_type'] == 'brand'])
        
        print(f"🎯 CEO article summaries: {ceo_count:,}")
        print(f"🏢 Brand article summaries: {brand_count:,}")
        print(f"📅 Date range: {summary_df['date'].min()} to {summary_df['date'].max()}")
        
        companies = summary_df['company'].nunique()
        print(f"🏭 Companies with negative coverage: {companies}")


def main():
    parser = argparse.ArgumentParser(
        description='Aggregate negative articles for stock chart visualization'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=90,
        help='Number of days to look back (default: 90)'
    )
    parser.add_argument(
        '--roster',
        type=str,
        default='rosters/main-roster.csv',
        help='Path to roster file (default: rosters/main-roster.csv)'
    )
    
    args = parser.parse_args()
    
    if args.days_back < 1:
        print("❌ --days-back must be at least 1")
        return 1
    
    create_negative_summary(days_back=args.days_back, roster_path=args.roster)
    return 0


if __name__ == "__main__":
    exit(main())
