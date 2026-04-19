"""
Merge cv.ee and cvkeskus vacancy datasets into one flat JSON file.

Rules:
- keep duplicates within the same site
- remove duplicates only across sites, based on cross_site_merge_plan.json
- when choosing which cross-site version to keep, prefer HTML records;
  if no HTML is present, use the best_record_id from the merge plan
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import load_json, save_json

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CV_FILE = DATA_DIR / "cv_jobs.json"
CVKESKUS_FILE = DATA_DIR / "cvkeskus_jobs.json"
MERGE_PLAN_FILE = DATA_DIR / "cross_site_merge_plan.json"
OUTPUT_FILE = DATA_DIR / "merged_jobs.json"


def join_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return "; ".join(parts)
    return str(value).strip()


def normalize_iso_date(value: str | None) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    return value[:10]


def normalize_estonian_date(value: str | None) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    parts = value.split(".")
    if len(parts) != 3:
        return value
    day, month, year = parts
    if len(year) == 4:
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return value


def record_uid(source: str, record_id) -> str:
    return f"{source}:{record_id}"


def normalize_cv_job(job: dict) -> dict:
    info = job.get("esmane_info") or {}
    return {
        "source": "cv.ee",
        "id": str(job.get("id", "")),
        "url": job.get("url", ""),
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "publishDate": normalize_iso_date(job.get("publishDate")),
        "expirationDate": normalize_iso_date(job.get("expirationDate")),
        "salary_info": join_value(info.get("salary_info")),
        "Asukoht": join_value(info.get("Asukoht")),
        "Tööaeg": join_value(info.get("Tööaeg")),
        "Kontaktisik": join_value(info.get("Kontaktisik")),
        "Lisainfo": "",
        "company_info": job.get("company_info", "") or "",
        "full_text": job.get("full_text", "") or "",
        "content_type": job.get("content_type", ""),
        "images": job.get("images", []) or [],
    }


def get_cvkeskus_salary(meta: dict) -> str:
    for key in (
        "Brutopalk",
        "Neto palk",
        "Netopalk",
        "Neto töötasu",
        "Brutokuupalk",
        "Brutto salary",
        "Gross salary",
        "Net salary",
        "Брутто зарплата",
        "Нетто зарплата",
    ):
        value = meta.get(key)
        if value:
            return str(value).strip()
    return ""


def get_first_meta(meta: dict, keys: tuple[str, ...]) -> str:
    for key in keys:
        value = meta.get(key)
        if value:
            return str(value).strip()
    return ""


def normalize_cvkeskus_job(job: dict) -> dict:
    meta = job.get("meta") or {}
    publish_date = get_first_meta(
        meta,
        ("Kuulutus sisestati", "Published", "Добавлено"),
    )
    expiration_date = get_first_meta(
        meta,
        ("Aegub", "Expires", "Заканчивается"),
    )
    location = get_first_meta(
        meta,
        ("Asukoht", "Location", "Локация"),
    )
    job_type = get_first_meta(
        meta,
        ("Töö tüüp", "Job type", "Вид работы"),
    )
    additional_info = get_first_meta(
        meta,
        ("Lisainfo", "Additional info", "Дополнительная информация"),
    )

    if "/" in publish_date:
        publish_date = publish_date.replace("/", ".")
    if "/" in expiration_date:
        expiration_date = expiration_date.replace("/", ".")

    return {
        "source": "cvkeskus",
        "id": str(job.get("id", "")),
        "url": job.get("url", ""),
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "publishDate": normalize_estonian_date(publish_date),
        "expirationDate": normalize_estonian_date(expiration_date),
        "salary_info": get_cvkeskus_salary(meta),
        "Asukoht": join_value(location),
        "Tööaeg": join_value(job_type),
        "Kontaktisik": "",
        "Lisainfo": join_value(additional_info),
        "company_info": "",
        "full_text": job.get("full_text", "") or "",
        "content_type": job.get("content_type", ""),
        "images": job.get("images", []) or [],
    }


def is_html_record(record: dict) -> bool:
    content_type = (record.get("content_type") or "").strip().lower()
    return "html" in content_type


def choose_canonical_record_id(cluster: dict, normalized_records: dict[str, dict]) -> str | None:
    record_ids = cluster.get("record_ids", [])
    html_candidates = [rid for rid in record_ids if is_html_record(normalized_records.get(rid, {}))]
    if html_candidates:
        html_candidates.sort()
        return html_candidates[0]
    best_record_id = cluster.get("best_record_id")
    if best_record_id in normalized_records:
        return best_record_id
    for rid in record_ids:
        if rid in normalized_records:
            return rid
    return None


def build_drop_set(merge_plan: list[dict], normalized_records: dict[str, dict]) -> set[str]:
    drop_ids = set()

    for group in merge_plan:
        if group.get("status") != "ok":
            continue

        for cluster in group.get("clusters", []):
            if not cluster.get("is_cross_site_duplicate"):
                continue

            record_ids = cluster.get("record_ids", [])
            canonical_id = choose_canonical_record_id(cluster, normalized_records)
            if not canonical_id:
                continue

            canonical_record = normalized_records.get(canonical_id)
            if not canonical_record:
                continue

            canonical_source = canonical_record.get("source")
            for rid in record_ids:
                record = normalized_records.get(rid)
                if not record:
                    continue
                if record.get("source") != canonical_source:
                    drop_ids.add(rid)

    return drop_ids


def main() -> None:
    cv_jobs = load_json(CV_FILE)
    cvkeskus_jobs = load_json(CVKESKUS_FILE)
    merge_plan = load_json(MERGE_PLAN_FILE)

    normalized_records: dict[str, dict] = {}
    merged_list: list[dict] = []

    for job in cv_jobs:
        normalized = normalize_cv_job(job)
        uid = record_uid("cv.ee", normalized["id"])
        normalized_records[uid] = normalized
        merged_list.append(normalized)

    for job in cvkeskus_jobs:
        normalized = normalize_cvkeskus_job(job)
        uid = record_uid("cvkeskus", normalized["id"])
        normalized_records[uid] = normalized
        merged_list.append(normalized)

    drop_ids = build_drop_set(merge_plan, normalized_records)
    final_jobs = [
        job
        for job in merged_list
        if record_uid(job["source"], job["id"]) not in drop_ids
    ]

    save_json(OUTPUT_FILE, final_jobs)

    log.info(f"cv.ee input records: {len(cv_jobs)}")
    log.info(f"cvkeskus input records: {len(cvkeskus_jobs)}")
    log.info(f"Combined input records: {len(merged_list)}")
    log.info(f"Cross-site records dropped: {len(drop_ids)}")
    log.info(f"Final merged records: {len(final_jobs)}")
    log.info(f"Saved merged dataset to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
