import argparse
import json
import re
from pathlib import Path

import pandas as pd


MAINLAND_REGION_MAP = {
    "黑龙江": "东北地区",
    "吉林": "东北地区",
    "辽宁": "东北地区",
    "北京": "华北地区",
    "天津": "华北地区",
    "河北": "华北地区",
    "山西": "华北地区",
    "山东": "华北地区",
    "河南": "华北地区",
    "上海": "东南地区",
    "江苏": "东南地区",
    "浙江": "东南地区",
    "安徽": "东南地区",
    "福建": "东南地区",
    "江西": "东南地区",
    "湖北": "东南地区",
    "湖南": "东南地区",
    "广东": "东南地区",
    "海南": "东南地区",
    "四川": "西南地区",
    "重庆": "西南地区",
    "贵州": "西南地区",
    "云南": "西南地区",
    "广西": "西南地区",
    "内蒙古": "西北地区",
    "陕西": "西北地区",
    "甘肃": "西北地区",
    "宁夏": "西北地区",
    "青海": "西北地区",
    "新疆": "西北地区",
    "西藏": "西北地区",
}

OTHER_CHINA = {"中国香港", "中国澳门", "中国台湾"}
CHINA_IP_WHITELIST = set(MAINLAND_REGION_MAP.keys()) | OTHER_CHINA

BRACKET_EMOJI_RE = re.compile(r"\[[^\]\r\n]+\]")
MENTION_DOUBAO_RE = re.compile(r"[@＠]\s*豆包")
MENTION_ANY_RE = re.compile(r"[@＠]\s*[^\s@＠,，。！？!?：:；;]+")
AT_SYMBOL_RE = re.compile(r"[@＠]+")
MULTI_SPACE_RE = re.compile(r"\s+")
MEANINGLESS_CONTENT_RE = re.compile(
    r"^(?:[1１]+|[6６]+|扣\s*[1１一]|(?:已\s*三连|一键\s*三连|三连)(?:了|啦)?)\s*[!！。,.，~～…]*$"
)

# Common emoji unicode ranges.
UNICODE_EMOJI_RE = re.compile(
    "["
    "\U0001F1E6-\U0001F1FF"
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "\uFE0F"
    "]+",
    flags=re.UNICODE,
)


def normalize_content(text: str) -> str:
    text = MENTION_ANY_RE.sub("", text)
    text = AT_SYMBOL_RE.sub("", text)
    text = BRACKET_EMOJI_RE.sub("", text)
    text = UNICODE_EMOJI_RE.sub("", text)
    text = MULTI_SPACE_RE.sub(" ", text).strip()
    return text


def is_meaningless_content(text: str) -> bool:
    return bool(MEANINGLESS_CONTENT_RE.fullmatch(text.strip()))


def map_region(ip_location: str) -> str:
    if ip_location in MAINLAND_REGION_MAP:
        return MAINLAND_REGION_MAP[ip_location]
    if ip_location in OTHER_CHINA:
        return "其他中国地区"
    return ""


def clean_comments(input_csv: Path, output_clean_csv: Path, output_non_china_csv: Path, output_report_json: Path) -> dict:
    df = pd.read_csv(input_csv)
    total_rows = len(df)

    df["content"] = df["content"].fillna("").astype(str)
    df["ip_location"] = df["ip_location"].fillna("").astype(str).str.strip()

    doubao_mask = df["content"].str.contains(MENTION_DOUBAO_RE, na=False)
    doubao_removed = int(doubao_mask.sum())
    work_df = df.loc[~doubao_mask].copy()

    original_content = work_df["content"].copy()
    work_df["content"] = work_df["content"].apply(normalize_content)
    content_changed_rows = int((original_content != work_df["content"]).sum())

    empty_content_mask = work_df["content"].str.strip().eq("")
    empty_content_removed = int(empty_content_mask.sum())
    work_df = work_df.loc[~empty_content_mask].copy()

    meaningless_mask = work_df["content"].apply(is_meaningless_content)
    meaningless_removed = int(meaningless_mask.sum())
    work_df = work_df.loc[~meaningless_mask].copy()

    ip_unknown_mask = work_df["ip_location"].eq("IP未知")
    ip_unknown_removed = int(ip_unknown_mask.sum())

    china_mask = work_df["ip_location"].isin(CHINA_IP_WHITELIST)
    non_china_df = work_df.loc[~china_mask | ip_unknown_mask].copy()
    cleaned_df = work_df.loc[china_mask & ~ip_unknown_mask].copy()

    cleaned_df["所属地区"] = cleaned_df["ip_location"].apply(map_region)

    ip_col_idx = cleaned_df.columns.get_loc("ip_location")
    ordered_cols = cleaned_df.columns.tolist()
    ordered_cols.insert(ip_col_idx + 1, ordered_cols.pop(ordered_cols.index("所属地区")))
    cleaned_df = cleaned_df[ordered_cols]

    output_clean_csv.parent.mkdir(parents=True, exist_ok=True)
    output_non_china_csv.parent.mkdir(parents=True, exist_ok=True)
    output_report_json.parent.mkdir(parents=True, exist_ok=True)

    cleaned_df.to_csv(output_clean_csv, index=False, encoding="utf-8-sig")
    non_china_df.to_csv(output_non_china_csv, index=False, encoding="utf-8-sig")

    report = {
        "input_file": str(input_csv).replace("\\", "/"),
        "output_clean_file": str(output_clean_csv).replace("\\", "/"),
        "output_non_china_file": str(output_non_china_csv).replace("\\", "/"),
        "total_rows": int(total_rows),
        "doubao_removed": doubao_removed,
        "ip_unknown_removed": ip_unknown_removed,
        "empty_content_removed": empty_content_removed,
        "meaningless_content_removed": meaningless_removed,
        "non_china_rows": int(len(non_china_df)),
        "cleaned_rows": int(len(cleaned_df)),
        "content_changed_rows": content_changed_rows,
        "region_distribution": cleaned_df["所属地区"].value_counts().to_dict(),
        "row_conservation_check": {
            "formula": "total_rows == doubao_removed + empty_content_removed + meaningless_content_removed + cleaned_rows + non_china_rows",
            "passed": int(total_rows)
            == doubao_removed + empty_content_removed + meaningless_removed + int(len(cleaned_df)) + int(len(non_china_df)),
        },
    }

    with output_report_json.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean douyin comment CSV by custom rules.")
    parser.add_argument(
        "--input",
        default="data/douyin/csv/search_comments_2026-04-15.csv",
        help="Input CSV path",
    )
    parser.add_argument(
        "--output-clean",
        default="data/douyin/csv/search_comments_2026-04-15_cleaned.csv",
        help="Output cleaned CSV path",
    )
    parser.add_argument(
        "--output-non-china",
        default="data/douyin/csv/search_comments_2026-04-15_non_china.csv",
        help="Output non-China CSV path",
    )
    parser.add_argument(
        "--output-report",
        default="data/douyin/csv/search_comments_2026-04-15_clean_report.json",
        help="Output report JSON path",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    report = clean_comments(
        input_csv=Path(args.input),
        output_clean_csv=Path(args.output_clean),
        output_non_china_csv=Path(args.output_non_china),
        output_report_json=Path(args.output_report),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
