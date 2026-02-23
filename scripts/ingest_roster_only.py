#!/usr/bin/env python3
import argparse

from gcs_utils import exists_gcs, open_gcs_text
from ingest_csvs import get_conn
from ingest_metrics import ingest_boards_csv
from ingest_v2 import upsert_companies_ceos


def parse_args():
    p = argparse.ArgumentParser(description="Ingest roster only (companies + CEOs).")
    p.add_argument(
        "--roster-path",
        default="gs://risk-dashboard/rosters/main-roster.csv",
        help="Path to main-roster.csv (local or gs://).",
    )
    p.add_argument(
        "--boards-path",
        default="gs://risk-dashboard/rosters/boards-roster.csv",
        help="Path to boards-roster.csv (local or gs://).",
    )
    return p.parse_args()


def open_text(path: str):
    if path.startswith("gs://"):
        return open_gcs_text(path)
    return open(path, "r", encoding="utf-8", newline="")


def main():
    args = parse_args()
    roster_path = args.roster_path
    boards_path = args.boards_path
    if roster_path.startswith("gs://") and not exists_gcs(roster_path):
        raise SystemExit(f"Roster not found: {roster_path}")
    if boards_path and boards_path.startswith("gs://") and not exists_gcs(boards_path):
        raise SystemExit(f"Boards roster not found: {boards_path}")
    conn = get_conn()
    try:
        with open_text(roster_path) as f:
            upsert_companies_ceos(conn, f)
        print(f"{roster_path}: upserted companies + CEOs")
        if boards_path:
            with open_text(boards_path) as f:
                count = ingest_boards_csv(conn, f)
            print(f"{boards_path}: ingested {count} boards rows")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
