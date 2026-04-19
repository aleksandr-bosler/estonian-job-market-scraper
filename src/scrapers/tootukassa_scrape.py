"""
Scrape job listings from tootukassa.ee via GraphQL API.
Reads vacancy IDs from sitemap, fetches full structured data for each vacancy.
"""

import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from xml.etree import ElementTree as ET

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "et,ru,en;q=0.8",
    "Origin": "https://www.tootukassa.ee",
    "Referer": "https://www.tootukassa.ee/et/toopakkumised",
}

LANG_PATH = "/et/toopakkumised/"
JOB_DELAY = 10

MAX_JOBS = None     # None = scrape all
OUTPUT_FILE = DATA_DIR / "tootukassa_jobs.json"
PROGRESS_FILE = DATA_DIR / "tootukassa_progress.json"
FAILED_JSON = DATA_DIR / "tootukassa_failed_ids.json"

SITEMAP_PAGES = [
    "https://www.tootukassa.ee/web/sitemaps/joboffers/sitemap.xml?page=1",
    "https://www.tootukassa.ee/web/sitemaps/joboffers/sitemap.xml?page=2",
    "https://www.tootukassa.ee/web/sitemaps/joboffers/sitemap.xml?page=3",
]


GRAPHQL_URL = "https://www.tootukassa.ee/web/graphql"
GRAPHQL_QUERY = """
query jobofferquery($id: Int!) {
  publicJobOfferQuery(jobOfferId: $id) {
    id
    empisId
    asutusId
    nimetus
    ametinimetusTapsustus
    staatusKood
    kandideerimineKp
    avalikKontaktisik {
      nimi
      email
      ametikoht
      telefon
    }
    tookohaAndmed {
      kohtadeArv
      onKodusTootamine
      toosuhteKestusKood
      tahtaegTapsustus
      koormus
      onOsakohaga
      onTaiskohaga
      onVahetustega
      onOositi
      tooaegTapsustus
      tooleAsuminePaev
      tooleAsumineKuu
      tooleAsumineAasta
      tootasuAlates
      tootasuKuni
      onPalkAvalik
      tootasuTapsustus
      tooylesanded
      omaltPooltPakume
    }
    noudedKandidaadile {
      varasemTookogemus
      haridusTase
      arvutiOskusTase
      arvutiOskusTapsustus
      nouded
      lisainfoKandideerijale
      linkKeskkonda
      muuDokumentKirjeldus
      onNoutudYksKeel
      hariduseValdkonnad
      kandideerimiseDokumendid
      ametitunnistused
      kutsetunnistused
      juhiload
      keeleoskused {
        onNoutud
        keel
        taseKirjas
        taseKones
      }
    }
    rekvisiidid {
      kinnitamiseKp
      lisamiseKp
    }
    aadressid {
      postiindeks
      aadressTekst
      aadressTapsustus
    }
    toopakkuja {
      nimi
      registrikood
      tutvustus
    }
    tolked {
      tolgeKood
      vaartusEn
    }
  }
}
""".strip()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_sitemap_urls(xml_text: str) -> list[str]:
    urls: list[str] = []
    root = ET.fromstring(xml_text)
    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    for loc in root.findall(f".//{ns}loc"):
        if loc.text:
            urls.append(loc.text.strip())
    return urls


def extract_vacancy_id(url: str) -> str | None:
    m = re.search(r"-([0-9]+)$", urlparse(url).path)
    return m.group(1) if m else None


def load_progress() -> set:
    """Loads already downloaded IDs - for resuming after a crash."""
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_progress(done_ids: set) -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(done_ids), f)


def graphql_joboffer(job_id: int, session: requests.Session) -> dict:
    payload = {
        "operationName": "jobofferquery",
        "variables": {"id": job_id},
        "query": GRAPHQL_QUERY,
    }
    resp = session.post(
        GRAPHQL_URL,
        headers={**HEADERS, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors for id={job_id}: {data['errors']}")
    return data.get("data", {}).get("publicJobOfferQuery") or {}


def build_record(job: dict, url: str) -> dict:
    """Saves all fields with original GraphQL field names."""
    record = dict(job)
    record["url"] = url

    raw_tolked = job.get("tolked") or []
    record["tolked"] = [
        {"tolgeKood": t["tolgeKood"], "vaartusEn": t["vaartusEn"]}
        for t in raw_tolked
        if t.get("vaartusEn") is not None
    ]

    return record


def main() -> None:
    session = make_session()
    done_ids = load_progress()

    if done_ids:
        log.info(f"Resuming: already collected {len(done_ids)} vacancies")

    sitemap_pages = SITEMAP_PAGES
    log.info(f"Using {len(sitemap_pages)} sitemap pages")
    all_urls: list[str] = []
    for sitemap_url in sitemap_pages:
        xml = session.get(sitemap_url, headers=HEADERS, timeout=30).text
        all_urls.extend(parse_sitemap_urls(xml))
        time.sleep(JOB_DELAY)

    seen_ids = set()
    unique_urls = []
    for u in all_urls:
        if LANG_PATH not in u:
            continue
        vid = extract_vacancy_id(u)
        if not vid or vid in seen_ids:
            continue
        seen_ids.add(vid)
        unique_urls.append(u)

    log.info(f"Found unique vacancies: {len(unique_urls)}")

    if isinstance(MAX_JOBS, int):
        unique_urls = unique_urls[:MAX_JOBS]

    if Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            results = json.load(f)
    else:
        results = []

    failed: list[str] = []

    for idx, url in enumerate(unique_urls, start=1):
        vid = extract_vacancy_id(url)
        if not vid or vid in done_ids:
            continue

        try:
            job = graphql_joboffer(int(vid), session)
            if not job:
                log.warning(f"[{idx}/{len(unique_urls)}] Empty response for id={vid}")
                failed.append(url)
                continue

            record = build_record(job, url)
            results.append(record)
            done_ids.add(vid)

            if len(done_ids) % 50 == 0:
                _flush(results, done_ids)
                log.info(f"Saved {len(results)} vacancies")

            log.info(f"[{idx}/{len(unique_urls)}] OK {record.get('nimetus')} (id={vid})")

        except Exception as e:
            log.error(f"[{idx}/{len(unique_urls)}] Error id={vid}: {e}")
            failed.append(url)

        if idx < len(unique_urls):
            time.sleep(JOB_DELAY)

    _flush(results, done_ids)

    if failed:
        with open(FAILED_JSON, "w", encoding="utf-8") as f:
            json.dump(failed, f, ensure_ascii=False, indent=2)
        log.warning(f"Failed to collect {len(failed)} vacancies - {FAILED_JSON}")

    log.info(f"Done. Total collected: {len(results)} vacancies - {OUTPUT_FILE}")


def _flush(results: list, done_ids: set) -> None:
    """Saves results and progress."""
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    save_progress(done_ids)


if __name__ == "__main__":
    main()