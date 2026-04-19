"""
Scrape job listings from cvkeskus.ee
Reads URLs from cv_keskus_job_urls.json, collects full_text + meta fields for each vacancy.
"""

import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import flush_state, load_done_ids, load_progress, load_results, safe_filename

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)",
    "Accept-Language": "et-EE,et;q=0.9",
}

BASE_URL = "https://www.cvkeskus.ee"
JOB_DELAY = 3.0

MAX_JOBS = None     # None = scrape all
URLS_FILE = DATA_DIR / "cvkeskus_job_urls.json"
OUTPUT_FILE = DATA_DIR / "cvkeskus_jobs.json"
PROGRESS_FILE = DATA_DIR / "cvkeskus_jobs_progress.json"
IMAGES_DIR = DATA_DIR / "images" / "cvkeskus"
SAVE_EVERY = 50


def parse_meta(soup: BeautifulSoup) -> dict:
    meta = {}
    aside = soup.select_one("aside")
    if not aside:
        return meta

    for row in aside.select("div.flex.gap-2\\.5"):
        divs = row.find_all("div", recursive=False)
        if len(divs) < 2:
            continue
        children = divs[1].find_all("div")
        if len(children) >= 2:
            label = children[0].get_text(strip=True)
            value = children[1].get_text(strip=True)
            if label:
                meta[label] = value

    return meta


def download_images(soup: BeautifulSoup, job_id: str, title: str, session: requests.Session) -> list[str]:
    saved = []
    imgs = soup.select("img[src*='/gfx/tpl_jobs/']")
    if not imgs:
        return saved

    Path(IMAGES_DIR).mkdir(exist_ok=True)
    clean_title = safe_filename(title) or job_id

    for i, img in enumerate(imgs):
        src = img.get("src", "")
        if not src:
            continue

        ext = src.rsplit(".", 1)[-1].split("?")[0] or "jpg"
        suffix = f"_{i+1}" if i > 0 else ""
        filename = f"{job_id}_{clean_title}{suffix}.{ext}"
        filepath = Path(IMAGES_DIR) / filename

        if filepath.exists():
            saved.append(filename)
            continue

        img_url = BASE_URL + src if src.startswith("/") else src
        try:
            r = session.get(img_url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            with filepath.open("wb") as f:
                f.write(r.content)
            saved.append(filename)
        except Exception as e:
            log.warning(f"Image download error ({img_url}): {e}")

    return saved


def get_full_text_html(soup: BeautifulSoup) -> tuple[str, str]:
    for el in soup.select("div.apply-button, div.notification-text, script, style"):
        el.decompose()

    job_offer = soup.select_one("div.job-offer")

    benefits_h3 = soup.find("h3", string=lambda t: t and "Pakutavad hüved" in t)
    benefits_text = ""
    if benefits_h3:
        parts = [benefits_h3.get_text(strip=True)]
        sib = benefits_h3.find_next_sibling()
        if sib:
            for span in sib.select("span:not(:has(svg))"):
                t = span.get_text(strip=True)
                if t:
                    parts.append(t)
        benefits_text = "\n".join(parts)

    if job_offer:
        text = job_offer.get_text(separator="\n", strip=True)
        has_images = bool(job_offer.select("img"))
        if benefits_text:
            text = text + "\n\n" + benefits_text
        if text:
            return text, "html"
        if has_images:
            return "", "image_only"

    return "", "unknown"


def get_full_text_iframe(soup: BeautifulSoup, session: requests.Session, job_url: str) -> tuple[str, str]:
    iframe = soup.select_one("iframe#htmlContentIframe")
    if not iframe:
        return "", ""

    src = iframe.get("src", "")
    if not src:
        return "", ""

    iframe_url = BASE_URL + src
    iframe_headers = {
        **HEADERS,
        "Referer": job_url,
        "sec-fetch-dest": "iframe",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        resp = session.get(iframe_url, headers=iframe_headers, timeout=15)
        resp.raise_for_status()
        iframe_soup = BeautifulSoup(resp.text, "html.parser")
        body = iframe_soup.find("body")
        if body:
            text = body.get_text(separator="\n", strip=True)
            return text, "iframe"
    except Exception as e:
        log.warning(f"Iframe fetch error: {e}")

    return "", "iframe_error"


def parse_job(job_id: str, url: str, session: requests.Session) -> dict:
    resp = session.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    title_el = soup.select_one("h1 .main-lang-block")
    title = title_el.get_text(strip=True) if title_el else ""

    company_el = soup.select_one("a[data-track*='emp_click_header']")
    company = company_el.get_text(strip=True) if company_el else ""

    meta = parse_meta(soup)

    full_text, content_type = get_full_text_html(soup)
    if not full_text:
        iframe_text, iframe_type = get_full_text_iframe(soup, session, url)
        if iframe_text:
            full_text = iframe_text
            content_type = iframe_type

    images = download_images(soup, job_id, title, session)

    return {
        "id": job_id,
        "url": url,
        "title": title,
        "company": company,
        "meta": meta,
        "full_text": full_text,
        "content_type": content_type,
        "images": images,
    }


def main():
    with open(URLS_FILE, encoding="utf-8") as f:
        all_urls = json.load(f)

    if MAX_JOBS:
        all_urls = all_urls[:MAX_JOBS]

    total = len(all_urls)
    log.info(f"Loaded {total} URLs from {URLS_FILE}")

    results = load_results(OUTPUT_FILE)
    done_ids = load_done_ids(results)
    next_index = load_progress(PROGRESS_FILE)
    processed_since_flush = 0

    if results:
        log.info(f"Resuming: {len(done_ids)} already scraped, {total - len(done_ids)} remaining")
    if next_index:
        log.info(f"Continuing from index {next_index}")

    session = requests.Session()

    for i in range(next_index, total):
        entry = all_urls[i]
        job_id = str(entry["job_id"])
        job_url = entry["url"]

        if job_id in done_ids:
            continue

        log.info(f"[{i+1}/{total}] {job_url}")

        try:
            job = parse_job(job_id, job_url, session)
            results.append(job)
            done_ids.add(job_id)
            processed_since_flush += 1
            log.info(f"  OK [{job['content_type']}] text_len={len(job['full_text'])}")
        except Exception as e:
            log.error(f"  Error: {e}")
            results.append({"id": job_id, "url": job_url, "error": str(e)})
            done_ids.add(job_id)
            processed_since_flush += 1

        next_index = i + 1
        if processed_since_flush >= SAVE_EVERY:
            flush_state(results, OUTPUT_FILE, PROGRESS_FILE, next_index)
            processed_since_flush = 0
            log.info(f"Checkpoint: saved {len(results)} jobs")

        time.sleep(JOB_DELAY)

    flush_state(results, OUTPUT_FILE, PROGRESS_FILE, total)
    log.info(f"Done. Saved {len(results)} jobs to {OUTPUT_FILE}")

    types = {}
    for j in results:
        t = j.get("content_type", "error")
        types[t] = types.get(t, 0) + 1
    log.info(f"Content types: {types}")


if __name__ == "__main__":
    main()
