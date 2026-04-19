"""
Compare vacancies from cvkeskus and cv.ee and find cross-site duplicates.

Matching is done by normalized title + company. Duplicates within the same site
are ignored; only cross-site matches are reported.
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

CVKESKUS_FILE = DATA_DIR / "cvkeskus_jobs.json"
CV_FILE = DATA_DIR / "cv_jobs.json"
OUTPUT_FILE = DATA_DIR / "cross_site_duplicates.json"



def build_key(job: dict) -> tuple[str, str]:
    return (
        normalize_text(job.get("title", "")),
        normalize_text(job.get("company", "")),
    )


def compact_job(job: dict, source: str) -> dict:
    return {
        "source": source,
        "id": job.get("id"),
        "url": job.get("url"),
        "title": job.get("title"),
        "company": job.get("company"),
        "content_type": job.get("content_type"),
        "full_text": job.get("full_text", ""),
    }


def main() -> None:
    cvkeskus_jobs = load_json(CVKESKUS_FILE)
    cv_jobs = load_json(CV_FILE)

    cvkeskus_index: dict[tuple[str, str], list[dict]] = {}
    for job in cvkeskus_jobs:
        key = build_key(job)
        if not all(key):
            continue
        cvkeskus_index.setdefault(key, []).append(job)

    cv_index: dict[tuple[str, str], list[dict]] = {}
    for job in cv_jobs:
        key = build_key(job)
        if not all(key):
            continue
        cv_index.setdefault(key, []).append(job)

    duplicate_groups = []
    duplicate_pairs = 0

    shared_keys = [key for key in cv_index if key in cvkeskus_index]

    for key in shared_keys:
        cv_matches = cv_index[key]
        cvkeskus_matches = cvkeskus_index[key]
        duplicate_pairs += len(cv_matches) * len(cvkeskus_matches)
        duplicate_groups.append(
            {
                "match_key": {
                    "title": cv_matches[0].get("title"),
                    "company": cv_matches[0].get("company"),
                },
                "cv_jobs": [compact_job(job, "cv.ee") for job in cv_matches],
                "cvkeskus_jobs": [compact_job(job, "cvkeskus") for job in cvkeskus_matches],
            }
        )

    with Path(OUTPUT_FILE).open("w", encoding="utf-8") as f:
        json.dump(duplicate_groups, f, ensure_ascii=False, indent=2)

    log.info(f"cvkeskus jobs: {len(cvkeskus_jobs)}")
    log.info(f"cv.ee jobs: {len(cv_jobs)}")
    log.info(f"Cross-site duplicate pairs: {duplicate_pairs}")
    log.info(f"Cross-site duplicate groups: {len(duplicate_groups)}")
    log.info(f"Saved matches to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
