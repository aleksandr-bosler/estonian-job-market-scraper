"""
Merge merged_jobs.json with tootukassa_jobs.json.

Rules:
- skip merged_jobs records with content_type == "unknown"
- normalize all output records to one English snake_case schema
- do not add tootukassa records that were reviewed as same_real_vacancy
- do not replace merged full_text with tootukassa text for duplicates
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import load_json, save_json

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MERGED_FILE = DATA_DIR / "merged_jobs.json"
TOOTUKASSA_FILE = DATA_DIR / "tootukassa_jobs.json"
REVIEW_FILE = DATA_DIR / "merged_vs_tootukassa_duplicates_reviewed.json"
OUTPUT_FILE = DATA_DIR / "merged_jobs_with_tootukassa.json"


def join_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return "; ".join(parts)
    return str(value).strip()


def build_tootukassa_full_text(job: dict) -> str:
    tookoha = job.get("tookohaAndmed") or {}
    nouded = job.get("noudedKandidaadile") or {}

    sections = (
        ("Tööülesanded", tookoha.get("tooylesanded")),
        ("Omalt poolt pakume", tookoha.get("omaltPooltPakume")),
        ("Nõuded kandidaadile", nouded.get("nouded")),
    )

    parts = []
    for label, value in sections:
        text = (value or "").strip()
        if text:
            parts.append(f"{label}\n{text}")

    return "\n\n".join(parts)


def build_tootukassa_salary_info(job: dict) -> str:
    tookoha = job.get("tookohaAndmed") or {}
    salary_from = tookoha.get("tootasuAlates")
    salary_to = tookoha.get("tootasuKuni")
    if salary_from is not None and salary_to is not None:
        return f"{salary_from} - {salary_to}€/kuus"
    return (tookoha.get("tootasuTapsustus") or "").strip()


def build_tootukassa_location(job: dict) -> str:
    aadressid = job.get("aadressid") or []
    return "; ".join(
        str(item.get("aadressTekst", "")).strip()
        for item in aadressid
        if str(item.get("aadressTekst", "")).strip()
    )


def normalize_merged_job(job: dict) -> dict:
    return {
        "source": job.get("source", ""),
        "id": str(job.get("id", "")),
        "url": job.get("url", ""),
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "vacancy_count": 1,
        "publish_date": (job.get("publishDate") or "").strip(),
        "expiration_date": (job.get("expirationDate") or "").strip(),
        "salary_info": (job.get("salary_info") or "").strip(),
        "location": (job.get("Asukoht") or "").strip(),
        "job_type": (job.get("Tööaeg") or "").strip(),
        "contact_person": (job.get("Kontaktisik") or "").strip(),
        "additional_info": (job.get("Lisainfo") or "").strip(),
        "company_info": (job.get("company_info") or "").strip(),
        "full_text": (job.get("full_text") or "").strip(),
        "content_type": (job.get("content_type") or "").strip(),
        "images": job.get("images", []) or [],
    }


def normalize_tootukassa_job(job: dict) -> dict:
    kontakt = job.get("avalikKontaktisik") or {}
    tookoha = job.get("tookohaAndmed") or {}
    toopakkuja = job.get("toopakkuja") or {}

    return {
        "source": "tootukassa.ee",
        "id": str(job.get("id", "")),
        "url": job.get("url", ""),
        "title": job.get("nimetus", ""),
        "company": toopakkuja.get("nimi", "") or "",
        "vacancy_count": tookoha.get("kohtadeArv") if tookoha.get("kohtadeArv") is not None else 1,
        "publish_date": (job.get("rekvisiidid") or {}).get("kinnitamiseKp", "") or "",
        "expiration_date": (job.get("kandideerimineKp") or "").strip(),
        "salary_info": build_tootukassa_salary_info(job),
        "location": build_tootukassa_location(job),
        "job_type": (tookoha.get("toosuhteKestusKood") or "").strip(),
        "contact_person": (kontakt.get("nimi") or "").strip(),
        "additional_info": (tookoha.get("tooaegTapsustus") or "").strip(),
        "company_info": "",
        "full_text": build_tootukassa_full_text(job),
        "content_type": "graphql",
        "images": [],
    }


def tootukassa_uid(job: dict) -> str:
    return f"tootukassa:{job.get('id')}"


def build_matched_tootukassa_ids(reviewed_groups: list[dict]) -> set[str]:
    matched_tootukassa_ids = set()

    for item in reviewed_groups:
        if item.get("status") != "ok":
            continue

        analysis = item.get("analysis") or {}
        for pair in analysis.get("pair_assessments", []):
            if pair.get("decision") != "same_real_vacancy":
                continue
            tootukassa_record_id = pair.get("tootukassa_record_id")
            if tootukassa_record_id:
                matched_tootukassa_ids.add(tootukassa_record_id)

    return matched_tootukassa_ids


def main() -> None:
    merged_jobs = load_json(MERGED_FILE)
    tootukassa_jobs = load_json(TOOTUKASSA_FILE)
    reviewed_groups = load_json(REVIEW_FILE)

    matched_tootukassa_ids = build_matched_tootukassa_ids(reviewed_groups)

    final_jobs = []
    kept_merged = 0
    skipped_unknown = 0

    for job in merged_jobs:
        if job.get("content_type") == "unknown":
            skipped_unknown += 1
            continue

        final_jobs.append(normalize_merged_job(job))
        kept_merged += 1

    added_tootukassa = 0
    for job in tootukassa_jobs:
        uid = tootukassa_uid(job)
        if uid in matched_tootukassa_ids:
            continue
        final_jobs.append(normalize_tootukassa_job(job))
        added_tootukassa += 1

    save_json(OUTPUT_FILE, final_jobs)

    log.info(f"Merged input records: {len(merged_jobs)}")
    log.info(f"Skipped merged unknown records: {skipped_unknown}")
    log.info(f"Kept merged records: {kept_merged}")
    log.info(f"Tootukassa input records: {len(tootukassa_jobs)}")
    log.info(f"Matched tootukassa duplicates not added: {len(matched_tootukassa_ids)}")
    log.info(f"Added tootukassa records: {added_tootukassa}")
    log.info(f"Final merged records: {len(final_jobs)}")
    log.info(f"Saved merged dataset to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
