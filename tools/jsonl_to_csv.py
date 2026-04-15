# -*- coding: utf-8 -*-
"""Convert Douyin JSONL output files to CSV.

Usage examples:
1) Convert default files under data/douyin/jsonl:
   python tools/jsonl_to_csv.py

2) Custom input/output:
   python tools/jsonl_to_csv.py \
       --contents-in data/douyin/jsonl/search_contents_2026-04-15.jsonl \
       --contents-out data/douyin/csv/search_contents_2026-04-15.csv \
       --comments-in data/douyin/jsonl/search_comments_2026-04-15.jsonl \
       --comments-out data/douyin/csv/search_comments_2026-04-15.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


# Field order based on current file format in this workspace.
CONTENT_FIELDS = [
    "aweme_id",
    "title",
    "desc",
    "create_time",
    "user_id",
    "nickname",
    "liked_count",
    "collected_count",
    "comment_count",
    "share_count",
    "ip_location",
]

COMMENT_FIELDS = [
    "comment_id",
    "aweme_id",
    "content",
    "create_time",
    "user_id",
    "like_count",
    "sub_comment_count",
    "ip_location",
]


def read_jsonl(path: Path) -> Tuple[List[Dict], int]:
    rows: List[Dict] = []
    invalid_lines = 0

    with path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            if isinstance(obj, dict):
                rows.append(obj)
            else:
                invalid_lines += 1

    return rows, invalid_lines


def write_csv(path: Path, rows: Iterable[Dict], fieldnames: List[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            # Fill missing fields with empty strings to keep stable schema.
            normalized = {k: row.get(k, "") for k in fieldnames}
            normalized["create_time"] = _format_create_time(normalized.get("create_time", ""))
            writer.writerow(normalized)
            count += 1

    return count


def _format_create_time(value: object) -> str:
    """Convert Unix timestamp to YYYY-MM-DD.

    Supports second and millisecond timestamps. If conversion fails,
    return the original value as string.
    """
    if value in (None, ""):
        return ""

    try:
        ts = float(value)
        # Millisecond timestamp fallback
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OverflowError):
        return str(value)


def convert_one(jsonl_path: Path, csv_path: Path, fields: List[str], label: str) -> None:
    if not jsonl_path.exists():
        print(f"[WARN] {label}: input file not found: {jsonl_path}")
        return

    rows, invalid = read_jsonl(jsonl_path)
    written = write_csv(csv_path, rows, fields)

    print(f"[OK] {label}: {written} rows written -> {csv_path}")
    if invalid:
        print(f"[WARN] {label}: skipped {invalid} invalid line(s)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert Douyin JSONL files to CSV.")

    parser.add_argument(
        "--contents-in",
        type=Path,
        default=Path("data/douyin/jsonl/search_contents_2026-04-15.jsonl"),
        help="Input JSONL file for contents.",
    )
    parser.add_argument(
        "--contents-out",
        type=Path,
        default=Path("data/douyin/csv/search_contents_2026-04-15.csv"),
        help="Output CSV file for contents.",
    )
    parser.add_argument(
        "--comments-in",
        type=Path,
        default=Path("data/douyin/jsonl/search_comments_2026-04-15.jsonl"),
        help="Input JSONL file for comments.",
    )
    parser.add_argument(
        "--comments-out",
        type=Path,
        default=Path("data/douyin/csv/search_comments_2026-04-15.csv"),
        help="Output CSV file for comments.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    convert_one(args.contents_in, args.contents_out, CONTENT_FIELDS, "contents")
    convert_one(args.comments_in, args.comments_out, COMMENT_FIELDS, "comments")


if __name__ == "__main__":
    main()
