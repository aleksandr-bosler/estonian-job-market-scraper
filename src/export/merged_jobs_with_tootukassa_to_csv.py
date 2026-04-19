"""
Export merged_jobs_with_tootukassa.json to CSV, excluding content_type and images.
"""

import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import load_json

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

INPUT_FILE = DATA_DIR / "merged_jobs_with_tootukassa.json"
OUTPUT_FILE = DATA_DIR / "merged_jobs_with_tootukassa.csv"
EXCLUDED_FIELDS = {"content_type", "images"}



def main() -> None:
    rows = load_json(INPUT_FILE)
    if not rows:
        raise ValueError(f"{INPUT_FILE} is empty")

    fieldnames = [key for key in rows[0].keys() if key not in EXCLUDED_FIELDS]

    with Path(OUTPUT_FILE).open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            filtered_row = {key: row.get(key, "") for key in fieldnames}
            writer.writerow(filtered_row)

    log.info(f"Input rows: {len(rows)}")
    log.info(f"Output columns: {len(fieldnames)}")
    log.info(f"Saved CSV to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
