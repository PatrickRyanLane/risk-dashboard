# Rosters Directory

This directory contains the consolidated roster files that serve as the single source of truth for all news sentiment tracking.

## Files

### `main-roster.csv`
**Primary roster file** containing all CEO and company information.

**Columns:**
- `CEO` - Full name of the CEO
- `Company` - Company name  
- `CEO Alias` - Search alias for news queries (typically CEO name or variation)
- `Website` - Company website
- `Stock` - Stock ticker symbol
- `Sector` - Industry sector

**Used by:**
- `scripts/news_articles_brands.py` - Extracts company names for brand news tracking
- `scripts/news_articles_ceos.py` - Uses CEO, Company, and CEO Alias for CEO news tracking
- `scripts/news_sentiment_ceos.py` - Aggregates sentiment data by CEO
- `scripts/process_serps.py` - CEO SERP processing
- `scripts/process_serps_brands.py` - Brand SERP processing

### `boards-roster.csv`
**Board members roster** for tracking board-level news and sentiment.

**Columns:**
- `Name` - Full name of the board member
- `Company` - Company name
- `Board Position` - Role on the board (Board Member, Chair, etc.)
- `Website` - Company website
- `Stock` - Stock ticker symbol
- `Sector` - Industry sector

**Status:** Template file - populate with actual board member data as needed.

## Migration Notes

This consolidation replaces the following deprecated files:
- ~~`brands.txt`~~ → Now extracted from `main-roster.csv` Company column
- ~~`ceo_aliases.csv`~~ → Now the CEO Alias column in `main-roster.csv`
- ~~`ceo_companies.csv`~~ → Now the CEO and Company columns in `main-roster.csv`
- ~~`data/roster.csv`~~ → Replaced by `main-roster.csv`

## Updating Data

To update the roster:
1. Edit `main-roster.csv` directly
2. Commit and push changes
3. The scripts will automatically use the updated data on the next run

No sync scripts are needed - all scripts read directly from these files.
