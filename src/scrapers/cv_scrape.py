"""
Scrape job listings from cv.ee.
Reads URLs from cv_job_urls.json, collects full_text plus selected side-panel fields.
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

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

BASE_URL = "https://cv.ee"
JOB_DELAY = 3.0

MAX_JOBS = None     # None = scrape all
URLS_FILE = DATA_DIR / "cv_job_urls.json"
OUTPUT_FILE = DATA_DIR / "cv_jobs.json"
PROGRESS_FILE = DATA_DIR / "cv_jobs_progress.json"
IMAGES_DIR = DATA_DIR / "images" / "cv"
SAVE_EVERY = 50


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_multiline_lines(lines: list[str]) -> str:
    filtered = [line.strip() for line in lines if line and line.strip()]
    return clean_text("\n".join(filtered))


def get_text_lines(node: BeautifulSoup | None) -> list[str]:
    if not node:
        return []

    text = node.get_text(separator="\n", strip=True)
    return [line.strip() for line in text.splitlines() if line.strip()]


def parse_esmane_info(soup: BeautifulSoup) -> dict:
    info = {}
    root = soup.select_one("div.vacancy-highlights")
    if not root:
        return info

    salary = root.select_one("div.vacancy-highlights__salary")
    if salary:
        salary_text = clean_text(salary.get_text(separator="\n", strip=True))
        salary_text = re.sub(
            r"Selle ametikoha keskmise palga info leiad aadressilt\s*palgad\.ee\.?",
            "",
            salary_text,
            flags=re.IGNORECASE,
        )
        salary_lines = [
            line
            for line in salary_text.splitlines()
            if line.strip() not in {"€", ".", "€."}
        ]
        salary_text = clean_multiline_lines(salary_lines)
        if salary_text:
            info["salary_info"] = salary_text

    for column in root.select("div.vacancy-highlights__section-column"):
        heading = column.find(["h3", "b"])
        if not heading:
            continue

        label = heading.get_text(strip=True)
        values = []
        for li in column.select("li"):
            value = clean_text(li.get_text(separator="\n", strip=True))
            if value:
                values.append(value)

        if not values:
            raw_lines = get_text_lines(column)
            values = [line for line in raw_lines if line != label]

        if label and values:
            info[label] = values

    return info


def parse_company_info(soup: BeautifulSoup) -> str:
    root = soup.select_one("div.vacancy-employer")
    if not root:
        return ""
    return clean_text(root.get_text(separator="\n", strip=True))



def download_images(soup: BeautifulSoup, job_id: str, title: str, session: requests.Session) -> list[str]:
    saved = []
    imgs = soup.select("div.vacancy-details__image img")
    if not imgs:
        return saved

    Path(IMAGES_DIR).mkdir(exist_ok=True)
    clean_title = safe_filename(title) or job_id

    for i, img in enumerate(imgs):
        src = img.get("src", "")
        if not src:
            continue

        ext = src.rsplit(".", 1)[-1].split("?")[0]
        if "/" in ext or not ext:
            ext = "jpg"

        suffix = f"_{i+1}" if i > 0 else ""
        filename = f"{job_id}_{clean_title}{suffix}.{ext}"
        filepath = Path(IMAGES_DIR) / filename

        if filepath.exists():
            saved.append(filename)
            continue

        img_url = urljoin(BASE_URL, src)
        try:
            r = session.get(img_url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            with filepath.open("wb") as f:
                f.write(r.content)
            saved.append(filename)
        except Exception as e:
            log.warning(f"Image download error ({img_url}): {e}")

    return saved


def download_iframe_images(
    soup: BeautifulSoup, job_id: str, title: str, session: requests.Session, job_url: str
) -> list[str]:
    iframe = soup.select_one("iframe.vacancy-content__url, iframe[title='urlDetails']")
    if not iframe:
        return []

    src = iframe.get("src", "")
    if not src:
        return []

    iframe_url = urljoin(BASE_URL, src)
    iframe_headers = {
        **HEADERS,
        "Referer": job_url,
        "sec-fetch-dest": "iframe",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "cross-site",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        resp = session.get(iframe_url, headers=iframe_headers, timeout=15)
        resp.raise_for_status()
        iframe_soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log.warning(f"Iframe image fetch error: {e}")
        return []

    saved = []
    Path(IMAGES_DIR).mkdir(exist_ok=True)
    clean_title = safe_filename(title) or job_id

    img = iframe_soup.select_one(
        "img#portalJobPublicationPagePanorama[src], img.panorama[src], meta[property='og:image'][content]"
    )
    if not img:
        return saved

    src = (img.get("src") or img.get("content") or "").strip()
    if not src:
        return saved

    img_url = urljoin(iframe_url, src)
    ext = img_url.rsplit(".", 1)[-1].split("?")[0]
    if "/" in ext or not ext:
        ext = "jpg"

    filename = f"{job_id}_{clean_title}_iframe_panorama.{ext}"
    filepath = Path(IMAGES_DIR) / filename

    if filepath.exists():
        return [filename]

    try:
        r = session.get(img_url, headers={**HEADERS, "Referer": iframe_url}, timeout=15)
        r.raise_for_status()
        with filepath.open("wb") as f:
            f.write(r.content)
        saved.append(filename)
    except Exception as e:
        log.warning(f"Iframe image download error ({img_url}): {e}")

    return saved


def get_full_text_html(soup: BeautifulSoup) -> tuple[str, str]:
    for el in soup.select("script, style"):
        el.decompose()

    details = soup.select_one("div.vacancy-details")
    if not details:
        return "", "unknown"

    has_images = bool(details.select("div.vacancy-details__image img"))
    sections = details.select("div.vacancy-details__section")
    text_parts = []

    for section in sections:
        section_text = clean_text(section.get_text(separator="\n", strip=True))
        if section_text:
            text_parts.append(section_text)

    if text_parts:
        content_type = "html+image" if has_images else "html"
        return "\n\n".join(text_parts), content_type
    if has_images:
        return "", "image_only"

    return "", "unknown"


def get_full_text_iframe(soup: BeautifulSoup, session: requests.Session, job_url: str) -> tuple[str, str]:
    iframe = soup.select_one("iframe.vacancy-content__url, iframe[title='urlDetails']")
    if not iframe:
        return "", ""

    src = iframe.get("src", "")
    if not src:
        return "", ""

    iframe_url = urljoin(BASE_URL, src)
    iframe_headers = {
        **HEADERS,
        "Referer": job_url,
        "sec-fetch-dest": "iframe",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "cross-site",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        resp = session.get(iframe_url, headers=iframe_headers, timeout=15)
        resp.raise_for_status()
        iframe_soup = BeautifulSoup(resp.text, "html.parser")
        body = iframe_soup.find("body")
        if body:
            raw_lines = [line.strip() for line in body.get_text(separator="\n", strip=True).splitlines()]
            junk_patterns = (
                r"^Kandideerige$",
                r"^powered by$",
                r"^d\.vinci$",
            )
            kept_lines = []
            for line in raw_lines:
                if any(re.fullmatch(pattern, line, flags=re.IGNORECASE) for pattern in junk_patterns):
                    continue
                kept_lines.append(line)
            text = clean_multiline_lines(kept_lines)
            if text:
                return text, "iframe"
    except Exception as e:
        log.warning(f"Iframe fetch error: {e}")

    return "", "iframe_error"


def parse_job(job_entry: dict, session: requests.Session) -> dict:
    job_id = str(job_entry["job_id"])
    url = job_entry["url"]

    resp = session.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    title = job_entry.get("positionTitle") or ""
    if not title:
        title_el = soup.select_one("h1")
        title = title_el.get_text(strip=True) if title_el else ""

    company = job_entry.get("employerName") or ""
    if not company:
        company_el = soup.select_one("a[href*='/search/employer/']")
        company = company_el.get_text(strip=True) if company_el else ""

    esmane_info = parse_esmane_info(soup)
    company_info = parse_company_info(soup)

    full_text, content_type = get_full_text_html(soup)
    if not full_text:
        iframe_text, iframe_type = get_full_text_iframe(soup, session, url)
        if iframe_text:
            full_text = iframe_text
            content_type = iframe_type
        elif iframe_type:
            content_type = iframe_type

    images = download_images(soup, job_id, title, session)
    if not full_text and soup.select_one("iframe.vacancy-content__url, iframe[title='urlDetails']"):
        iframe_images = download_iframe_images(soup, job_id, title, session, url)
        for image_name in iframe_images:
            if image_name not in images:
                images.append(image_name)
        if iframe_images and content_type == "iframe_error":
            content_type = "iframe_image_only"

    return {
        "id": job_id,
        "url": url,
        "title": title,
        "company": company,
        "publishDate": job_entry.get("publishDate"),
        "expirationDate": job_entry.get("expirationDate"),
        "esmane_info": esmane_info,
        "company_info": company_info,
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
            job = parse_job(entry, session)
            results.append(job)
            done_ids.add(job_id)
            processed_since_flush += 1
            log.info(f"  OK [{job['content_type']}] text_len={len(job['full_text'])} images={len(job['images'])}")
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
