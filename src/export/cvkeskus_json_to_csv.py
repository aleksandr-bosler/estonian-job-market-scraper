"""
Export cvkeskus_jobs.json to CSV, normalizing meta field names across languages.
"""

import csv
import json
import logging
import sys
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

META_KEY_ALIASES = {
    "Kuulutus sisestati": "Kuulutus sisestati",
    "Published": "Kuulutus sisestati",
    "Добавлено": "Kuulutus sisestati",
    "Aegub": "Aegub",
    "Expires": "Aegub",
    "Заканчивается": "Aegub",
    "Asukoht": "Asukoht",
    "Location": "Asukoht",
    "Локация": "Asukoht",
    "Töö tüüp": "Töö tüüp",
    "Job type": "Töö tüüp",
    "Вид работы": "Töö tüüp",
    "Lisainfo": "Lisainfo",
    "Additional info": "Lisainfo",
    "Дополнительная информация": "Lisainfo",
    "Brutopalk": "Brutopalk",
    "Gross salary": "Brutopalk",
    "Брутто зарплата": "Brutopalk",
    "Neto palk": "Neto palk",
    "Net salary": "Neto palk",
    "Нетто зарплата": "Neto palk",
    "Neto palkpalk": "Neto palk",
}

META_FIELD_ORDER = [
    "Kuulutus sisestati",
    "Aegub",
    "Asukoht",
    "Töö tüüp",
    "Lisainfo",
    "Brutopalk",
    "Neto palk",
]


def normalize_meta(meta):
    normalized = {}
    for key, value in (meta or {}).items():
        canonical_key = META_KEY_ALIASES.get(key, key)
        normalized[canonical_key] = value
    return normalized


def flatten_vacancy(item):
    meta = normalize_meta(item.get("meta"))
    images = item.get("images") or []

    row = {
        "id": item.get("id", ""),
        "url": item.get("url", ""),
        "title": item.get("title", ""),
        "company": item.get("company", ""),
        "content_type": item.get("content_type", ""),
        "full_text": item.get("full_text", ""),
        "images": "; ".join(str(image) for image in images),
    }

    for key in META_FIELD_ORDER:
        row[key] = meta.get(key, "")

    return row


def default_output_path(input_path):
    return input_path.with_suffix(".csv")


def main():
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DATA_DIR / "cvkeskus_jobs.json"
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else default_output_path(input_path)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected top-level JSON array with vacancies.")

    if not data:
        log.warning("No data found.")
        return

    filtered_data = [item for item in data if item.get("content_type") != "unknown"]
    rows = [flatten_vacancy(item) for item in filtered_data]

    fieldnames = [
        "id",
        "url",
        "title",
        "company",
        *META_FIELD_ORDER,
        "full_text",
        "content_type",
        "images",
    ]

    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    skipped_count = len(data) - len(filtered_data)
    log.info(f"Done. {len(rows)} vacancies saved to {output_path} ({skipped_count} skipped with content_type=unknown)")


if __name__ == "__main__":
    main()
