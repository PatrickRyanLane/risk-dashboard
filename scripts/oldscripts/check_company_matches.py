#!/usr/bin/env python3
"""
Diagnostic tool to identify company name mismatches between brand articles and roster.

This helps you see exactly which companies in your brand articles aren't matching
the roster, so you can fix them if needed.

Usage:
    python check_company_matches.py [--date 2025-10-12]
"""

import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import re


def normalize_company_name(name):
    """Normalize company names for matching."""
    if not name or name == 'nan':
        return ''
    
    name = str(name).strip()
    
    suffixes = [
        ' Inc.', ' Inc', ' Corporation', ' Corp.', ' Corp',
        ' Company', ' Co.', ' Co', ' LLC', ' L.L.C.', ' LP', ' L.P.',
        ' Ltd.', ' Ltd', ' Limited', ' PLC', ' plc',
        '.com', '.net', '.org'
    ]
    
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    
    name = re.sub(r'[^\w\s]', '', name)
    name = ' '.join(name.split())
    
    return name


def main():
    parser = argparse.ArgumentParser(description='Check company name matching')
    parser.add_argument('--date', default=None, help='Date to check (YYYY-MM-DD)')
    parser.add_argument('--roster', default='rosters/main-roster.csv', help='Roster path')
    args = parser.parse_args()
    
    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Load roster
    print(f"📋 Loading roster from {args.roster}...")
    try:
        roster_df = pd.read_csv(args.roster, encoding='utf-8-sig')
        roster_df.columns = [c.strip().lower() for c in roster_df.columns]
        roster_df['company'] = roster_df['company'].astype(str).str.strip()
        roster_df = roster_df[roster_df['company'] != '']
        roster_companies = set(roster_df['company'].tolist())
        print(f"   Found {len(roster_companies)} companies in roster\n")
    except Exception as e:
        print(f"❌ Error loading roster: {e}")
        return 1
    
    # Load brand articles
    brand_file = Path(f"data/processed_articles/{date}-brand-articles-modal.csv")
    print(f"📄 Loading brand articles from {brand_file}...")
    
    if not brand_file.exists():
        print(f"❌ File not found: {brand_file}")
        return 1
    
    try:
        brand_df = pd.read_csv(brand_file)
        brand_df.columns = [c.lower().strip() for c in brand_df.columns]
        
        if 'company' not in brand_df.columns:
            print("❌ Brand file missing 'company' column")
            return 1
        
        brand_df['company'] = brand_df['company'].astype(str).str.strip()
        brand_df = brand_df[brand_df['company'] != '']
        
        brand_companies = brand_df['company'].unique()
        print(f"   Found {len(brand_companies)} unique companies in brand file\n")
        
    except Exception as e:
        print(f"❌ Error loading brand file: {e}")
        return 1
    
    # Check matches
    print("="*80)
    print("COMPANY NAME MATCHING ANALYSIS")
    print("="*80)
    
    exact_matches = []
    fuzzy_matches = []
    no_matches = []
    
    for brand_company in sorted(brand_companies):
        # Try exact match
        if brand_company in roster_companies:
            exact_matches.append(brand_company)
            continue
        
        # Try case-insensitive
        found = False
        for roster_company in roster_companies:
            if roster_company.lower() == brand_company.lower():
                fuzzy_matches.append((brand_company, roster_company, 'case-insensitive'))
                found = True
                break
        
        if found:
            continue
        
        # Try normalized
        brand_norm = normalize_company_name(brand_company).lower()
        for roster_company in roster_companies:
            roster_norm = normalize_company_name(roster_company).lower()
            if brand_norm == roster_norm:
                fuzzy_matches.append((brand_company, roster_company, 'normalized'))
                found = True
                break
        
        if found:
            continue
        
        # Try partial match
        for roster_company in roster_companies:
            if (brand_company.lower() in roster_company.lower() or 
                roster_company.lower() in brand_company.lower()):
                if len(brand_company) >= 3 and len(roster_company) >= 3:
                    fuzzy_matches.append((brand_company, roster_company, 'partial'))
                    found = True
                    break
        
        if not found:
            no_matches.append(brand_company)
    
    # Print results
    print(f"\n✅ EXACT MATCHES: {len(exact_matches)}")
    if exact_matches and len(exact_matches) <= 20:
        for company in exact_matches:
            print(f"   • {company}")
    elif len(exact_matches) > 20:
        print(f"   (showing first 10 of {len(exact_matches)})")
        for company in exact_matches[:10]:
            print(f"   • {company}")
    
    print(f"\n🔄 FUZZY MATCHES: {len(fuzzy_matches)}")
    for brand, roster, match_type in fuzzy_matches:
        print(f"   • '{brand}' → '{roster}' ({match_type})")
    
    print(f"\n❌ NO MATCHES: {len(no_matches)}")
    if no_matches:
        print("\nThese companies in brand articles have NO match in roster:")
        for company in no_matches:
            print(f"   • {company}")
            # Try to suggest close matches
            norm = normalize_company_name(company).lower()
            suggestions = []
            for roster_company in roster_companies:
                roster_norm = normalize_company_name(roster_company).lower()
                if norm in roster_norm or roster_norm in norm:
                    suggestions.append(roster_company)
            if suggestions:
                print(f"      Possible matches: {', '.join(suggestions[:3])}")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print('='*80)
    total = len(brand_companies)
    matched = len(exact_matches) + len(fuzzy_matches)
    print(f"Total brand companies: {total}")
    print(f"Matched (will appear in heatmap): {matched} ({matched/total*100:.1f}%)")
    print(f"Unmatched (will be skipped): {len(no_matches)} ({len(no_matches)/total*100:.1f}%)")
    
    if no_matches:
        print(f"\n💡 TIP: To fix unmatched companies, either:")
        print(f"   1. Update company names in brand articles to match roster exactly")
        print(f"   2. Add these companies to your roster file")
        print(f"   3. Add manual mappings to the aggregator script")
    
    return 0


if __name__ == "__main__":
    exit(main())
