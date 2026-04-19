"""
Collect vacancy URLs from cv.ee search pagination.
"""

import json
import logging
import time
from pathlib import Path

import requests

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)",
    "Accept-Language": "et-EE,et;q=0.9",
}

BASE_URL = "https://cv.ee"
SEARCH_API_URL = f"{BASE_URL}/api/v1/vacancy-search-service/search"
STEP = 20
DELAY = 4.0
MAX_URLS = None  # None = no limit
OUTPUT_FILE = DATA_DIR / "cv_job_urls.json"
PROGRESS_FILE = DATA_DIR / "cv_url_progress.json"
PROCESSED_PAGES_FILE = DATA_DIR / "cv_processed_listing_pages.json"
SAVE_EVERY = 50


def scrape_page(start: int) -> list[dict]:
    url = (
        f"{SEARCH_API_URL}?limit={STEP}&offset={start}"
        "&sorting=RELEVANCE&showHidden=true"
    )
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data.get("vacancies", []):
        vacancy_id = item.get("id")
        if not vacancy_id:
            continue

        jobs.append(
            {
                "url": f"{BASE_URL}/et/vacancy/{vacancy_id}",
                "job_id": str(vacancy_id),
                "positionTitle": item.get("positionTitle"),
                "employerName": item.get("employerName"),
                "publishDate": item.get("publishDate"),
                "expirationDate": item.get("expirationDate"),
            }
        )

    return jobs


def listing_page_url(start: int) -> str:
    return (
        f"{SEARCH_API_URL}?limit={STEP}&offset={start}"
        "&sorting=RELEVANCE&showHidden=true"
    )


def load_existing_jobs(output_file) -> dict[str, dict]:
    """Load already saved vacancy URLs so collection can resume safely."""
    path = Path(output_file)
    if not path.exists():
        return {}

    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    jobs: dict[str, dict] = {}
    for item in data:
        job_id = item.get("job_id")
        if job_id:
            jobs[job_id] = item
    return jobs


def load_progress(progress_file) -> int:
    """Load pagination offset for resuming after interruption."""
    path = Path(progress_file)
    if not path.exists():
        return 0

    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    return int(data.get("next_start", 0))


def save_progress(progress_file, next_start: int) -> None:
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump({"next_start": next_start}, f, ensure_ascii=False, indent=2)


def load_processed_pages(processed_pages_file) -> list[str]:
    path = Path(processed_pages_file)
    if not path.exists():
        return []

    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    return [page for page in data if isinstance(page, str)]


def save_processed_pages(processed_pages: list[str], processed_pages_file) -> None:
    with open(processed_pages_file, "w", encoding="utf-8") as f:
        json.dump(processed_pages, f, ensure_ascii=False, indent=2)


def flush_state(all_jobs: dict[str, dict], output_file, next_start: int) -> None:
    result = list(all_jobs.values())
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    save_progress(PROGRESS_FILE, next_start)


def collect_all_urls(output_file=OUTPUT_FILE, max_urls: int | None = MAX_URLS):
    all_jobs = load_existing_jobs(output_file)
    processed_pages = load_processed_pages(PROCESSED_PAGES_FILE)
    processed_pages_set = set(processed_pages)
    start = load_progress(PROGRESS_FILE)
    last_saved_total = len(all_jobs)

    log.info("Starting vacancy URL collection")
    if isinstance(max_urls, int):
        log.info(f"Collection limit: {max_urls} URLs")
    if all_jobs:
        log.info(f"Resuming with {len(all_jobs)} already saved URLs")
    if start:
        log.info(f"Continuing from start={start}")

    while True:
        log.info(f"start={start}")

        try:
            jobs = scrape_page(start)
        except Exception as e:
            log.error(f"ERROR: {e}")
            break

        if not jobs:
            log.info("Empty page, stopping.")
            break

        new_count = 0
        for job in jobs:
            jid = job["job_id"]
            if jid not in all_jobs:
                all_jobs[jid] = job
                new_count += 1

                if len(all_jobs) - last_saved_total >= SAVE_EVERY:
                    flush_state(all_jobs, output_file, start)
                    last_saved_total = len(all_jobs)
                    log.info(f"Checkpoint: saved {len(all_jobs)} URLs")

                if isinstance(max_urls, int) and len(all_jobs) >= max_urls:
                    break

        log.info(f"{len(jobs)} vacancies ({new_count} new), total: {len(all_jobs)}")

        page_url = listing_page_url(start)
        if page_url not in processed_pages_set:
            processed_pages.append(page_url)
            processed_pages_set.add(page_url)
            save_processed_pages(processed_pages, PROCESSED_PAGES_FILE)

        next_start = start + STEP
        flush_state(all_jobs, output_file, next_start)
        last_saved_total = len(all_jobs)

        if isinstance(max_urls, int) and len(all_jobs) >= max_urls:
            log.info("Limit reached, stopping.")
            break

        if new_count == 0:
            log.info("All vacancies already collected, stopping.")
            break

        start = next_start
        time.sleep(DELAY)

    result = list(all_jobs.values())
    flush_state(all_jobs, output_file, start)

    log.info(f"Done. Saved {len(result)} vacancies to {output_file}")
    return result


if __name__ == "__main__":
    collect_all_urls(OUTPUT_FILE, max_urls=MAX_URLS)
