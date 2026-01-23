#!/usr/bin/env python3
"""
Estimate daily LLM call volume from processed CSV outputs.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd


def load_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def summarize(df: pd.DataFrame) -> Dict[str, int]:
    total = len(df)
    finance = int(df.get("finance_routine", pd.Series([False] * total)).fillna(False).astype(bool).sum())
    uncertain = int(df.get("uncertain", pd.Series([False] * total)).fillna(False).astype(bool).sum())
    return {"total": total, "finance_routine": finance, "uncertain": uncertain}


def format_row(label: str, stats: Dict[str, int]) -> str:
    total = stats["total"] or 1
    pct_uncertain = (stats["uncertain"] / total) * 100.0
    return (
        f"{label}: total={stats['total']}, "
        f"finance_routine={stats['finance_routine']}, "
        f"uncertain={stats['uncertain']} ({pct_uncertain:.1f}%)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate daily LLM call volume from CSV outputs.")
    parser.add_argument("--date", required=True, help="Date string used in filenames (YYYY-MM-DD).")
    parser.add_argument("--data-dir", default="data", help="Base data directory (default: data).")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    dstr = args.date

    patterns = [
        ("brand articles", data_dir / "processed_articles" / f"{dstr}-brand-articles-modal.csv"),
        ("ceo articles", data_dir / "processed_articles" / f"{dstr}-ceo-articles-modal.csv"),
        ("brand serps", data_dir / "processed_serps" / f"{dstr}-brand-serps-modal.csv"),
        ("ceo serps", data_dir / "processed_serps" / f"{dstr}-ceo-serps-modal.csv"),
    ]

    rows: List[str] = []
    totals = {"total": 0, "finance_routine": 0, "uncertain": 0}
    for label, path in patterns:
        df = load_csv(path)
        stats = summarize(df)
        rows.append(format_row(label, stats))
        for k in totals:
            totals[k] += stats[k]

    print("LLM usage estimate:")
    for row in rows:
        print("-", row)
    print("-", format_row("all", totals))
    print("Note: expected LLM calls ~= total uncertain rows (no hard cap).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
