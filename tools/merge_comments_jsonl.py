# -*- coding: utf-8 -*-
"""Merge comment JSONL files into target file with de-duplication.

Default behavior:
- target: data/douyin/jsonl/search_comments_2026-04-15.jsonl
- source: data/douyin/jsonl/search_comments_2026-04-15-1.jsonl

Example:
    python tools/merge_comments_jsonl.py

Custom paths:
    python tools/merge_comments_jsonl.py \
        --target data/douyin/jsonl/search_comments_2026-04-15.jsonl \
        --source data/douyin/jsonl/search_comments_2026-04-15-1.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple


def read_jsonl(path: Path) -> Tuple[List[Dict], int]:
    rows: List[Dict] = []
    invalid = 0

    if not path.exists():
        return rows, invalid

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                invalid += 1
                continue
            if isinstance(obj, dict):
                rows.append(obj)
            else:
                invalid += 1

    return rows, invalid


def dedupe_key(item: Dict) -> str:
    """Use comment_id as primary key; fallback to full JSON string."""
    comment_id = item.get("comment_id")
    if comment_id is None:
        return json.dumps(item, ensure_ascii=False, sort_keys=True)
    return str(comment_id)


def merge_comments(target_path: Path, source_path: Path) -> None:
    target_rows, target_invalid = read_jsonl(target_path)
    source_rows, source_invalid = read_jsonl(source_path)

    if not source_path.exists():
        print(f"[WARN] Source file not found: {source_path}")
        return

    seen: Set[str] = set()
    merged: List[Dict] = []

    for row in target_rows:
        key = dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    added = 0
    skipped_dup = 0
    for row in source_rows:
        key = dedupe_key(row)
        if key in seen:
            skipped_dup += 1
            continue
        seen.add(key)
        merged.append(row)
        added += 1

    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as f:
        for row in merged:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"[OK] Merge complete: {target_path}")
    print(f"[INFO] target original rows: {len(target_rows)}")
    print(f"[INFO] source rows: {len(source_rows)}")
    print(f"[INFO] added rows: {added}")
    print(f"[INFO] skipped duplicate rows: {skipped_dup}")
    if target_invalid or source_invalid:
        print(
            f"[WARN] skipped invalid lines - target: {target_invalid}, source: {source_invalid}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge comment JSONL files into a target JSONL file")
    parser.add_argument(
        "--target",
        type=Path,
        default=Path("data/douyin/jsonl/search_comments_2026-04-15.jsonl"),
        help="Target JSONL file to receive merged results",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("data/douyin/jsonl/search_comments_2026-04-15-1.jsonl"),
        help="Source JSONL file to merge into target",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merge_comments(args.target, args.source)


if __name__ == "__main__":
    main()
