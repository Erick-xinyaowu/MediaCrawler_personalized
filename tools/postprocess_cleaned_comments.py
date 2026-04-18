import argparse
import json
import re
from pathlib import Path

import pandas as pd


# Contact and conversion signals for ad/promo tagging.
AD_PATTERNS = {
    "contact_wechat": re.compile(r"(加\s*[微vV]|微信|vx|v\s*[:：])", re.IGNORECASE),
    "contact_qq": re.compile(r"(qq|QQ群|q群)", re.IGNORECASE),
    "contact_link": re.compile(r"(http[s]?://|www\.|链接|网址)", re.IGNORECASE),
    "contact_private": re.compile(r"(私信|主页|进群|扫码|二维码)", re.IGNORECASE),
    "conversion": re.compile(r"(下单|咨询|代理|返利|带货|资料领取|加群|联系我|加我)", re.IGNORECASE),
}


def tag_ad_promo(text: str) -> tuple[bool, str]:
    text = str(text or "")
    hits = [name for name, pattern in AD_PATTERNS.items() if pattern.search(text)]

    has_contact_signal = any(
        key in hits for key in ("contact_wechat", "contact_qq", "contact_link", "contact_private")
    )
    has_conversion_signal = "conversion" in hits

    # Treat as ad/promo only when there is contact/navigation signal,
    # or conversion terms appear together with contact-private guidance.
    is_ad = has_contact_signal or (has_conversion_signal and "contact_private" in hits)
    return (is_ad, "|".join(hits))


def process_file(input_csv: Path, output_csv: Path, report_json: Path) -> dict:
    df = pd.read_csv(input_csv)
    total_before = len(df)

    # Strict duplicate definition: same aweme_id + same content.
    dedup_df = df.drop_duplicates(subset=["aweme_id", "content"], keep="first").copy()
    strict_dedup_removed = total_before - len(dedup_df)

    tagged = dedup_df["content"].fillna("").astype(str).apply(tag_ad_promo)
    dedup_df["is_ad_promo"] = tagged.map(lambda x: x[0])
    dedup_df["ad_hit_keywords"] = tagged.map(lambda x: x[1])

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)

    dedup_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    report = {
        "input_file": str(input_csv).replace("\\", "/"),
        "output_file": str(output_csv).replace("\\", "/"),
        "rows_before": int(total_before),
        "strict_dedup_removed": int(strict_dedup_removed),
        "rows_after_dedup": int(len(dedup_df)),
        "ad_promo_tagged": int(dedup_df["is_ad_promo"].sum()),
        "ad_hit_distribution": dedup_df.loc[dedup_df["is_ad_promo"], "ad_hit_keywords"].value_counts().head(20).to_dict(),
    }

    with report_json.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Post-process cleaned comment CSV.")
    parser.add_argument(
        "--input",
        default="data/douyin/csv/search_comments_2026-04-15_cleaned.csv",
        help="Input cleaned CSV path",
    )
    parser.add_argument(
        "--output",
        default="data/douyin/csv/search_comments_2026-04-15_cleaned_dedup_adtag.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--report",
        default="data/douyin/csv/search_comments_2026-04-15_cleaned_dedup_adtag_report.json",
        help="Output report JSON path",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    report = process_file(
        input_csv=Path(args.input),
        output_csv=Path(args.output),
        report_json=Path(args.report),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
