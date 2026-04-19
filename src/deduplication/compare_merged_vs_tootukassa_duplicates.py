"""
Compare merged cv.ee/cvkeskus vacancies against tootukassa vacancies
and find cross-source duplicates by normalized title + company.
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import load_json
from utils.text import normalize_text

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MERGED_FILE = DATA_DIR / "merged_jobs.json"
TOOTUKASSA_FILE = DATA_DIR / "tootukassa_jobs.json"
OUTPUT_FILE = DATA_DIR / "merged_vs_tootukassa_duplicates.json"



def build_merged_key(job: dict) -> tuple[str, str]:
    return (
        normalize_text(job.get("title", "")),
        normalize_text(job.get("company", "")),
    )


def build_tootukassa_key(job: dict) -> tuple[str, str]:
    employer = (job.get("toopakkuja") or {}).get("nimi", "")
    return (
        normalize_text(job.get("nimetus", "")),
        normalize_text(employer),
    )


def compact_merged_job(job: dict) -> dict:
    return {
        "source": job.get("source", "merged"),
        "id": job.get("id"),
        "url": job.get("url"),
        "title": job.get("title"),
        "company": job.get("company"),
        "content_type": job.get("content_type"),
        "full_text": job.get("full_text", ""),
    }


def compact_tootukassa_job(job: dict) -> dict:
    return {
        "source": "tootukassa",
        "id": job.get("id"),
        "url": job.get("url"),
        "title": job.get("nimetus"),
        "company": (job.get("toopakkuja") or {}).get("nimi"),
        "content_type": "structured",
        "full_text": "",
        "raw_excerpt": {
            "ametinimetusTapsustus": job.get("ametinimetusTapsustus"),
            "tooylesanded": ((job.get("tookohaAndmed") or {}).get("tooylesanded")),
            "omaltPooltPakume": ((job.get("tookohaAndmed") or {}).get("omaltPooltPakume")),
            "nouded": ((job.get("noudedKandidaadile") or {}).get("nouded")),
        },
    }


def main() -> None:
    merged_jobs = load_json(MERGED_FILE)
    tootukassa_jobs = load_json(TOOTUKASSA_FILE)

    merged_index: dict[tuple[str, str], list[dict]] = {}
    for job in merged_jobs:
        key = build_merged_key(job)
        if not all(key):
            continue
        merged_index.setdefault(key, []).append(job)

    tootukassa_index: dict[tuple[str, str], list[dict]] = {}
    for job in tootukassa_jobs:
        key = build_tootukassa_key(job)
        if not all(key):
            continue
        tootukassa_index.setdefault(key, []).append(job)

    shared_keys = [key for key in merged_index if key in tootukassa_index]

    duplicate_groups = []
    duplicate_pairs = 0

    for key in shared_keys:
        merged_matches = merged_index[key]
        tootukassa_matches = tootukassa_index[key]
        duplicate_pairs += len(merged_matches) * len(tootukassa_matches)
        duplicate_groups.append(
            {
                "match_key": {
                    "title": merged_matches[0].get("title"),
                    "company": merged_matches[0].get("company"),
                },
                "merged_jobs": [compact_merged_job(job) for job in merged_matches],
                "tootukassa_jobs": [compact_tootukassa_job(job) for job in tootukassa_matches],
            }
        )

    with Path(OUTPUT_FILE).open("w", encoding="utf-8") as f:
        json.dump(duplicate_groups, f, ensure_ascii=False, indent=2)

    log.info(f"Merged jobs: {len(merged_jobs)}")
    log.info(f"Tootukassa jobs: {len(tootukassa_jobs)}")
    log.info(f"Cross-source duplicate pairs: {duplicate_pairs}")
    log.info(f"Cross-source duplicate groups: {len(duplicate_groups)}")
    log.info(f"Saved matches to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
