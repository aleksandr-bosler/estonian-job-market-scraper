"""
Review cross-site duplicate candidates with a local Qwen model via LM Studio.

For each group in cross_site_duplicates.json, the script asks the model to:
- cluster records that represent the same real vacancy
- decide whether the group contains true cross-site duplicates
- choose the best record in each cluster as the canonical version

Outputs:
- cross_site_duplicates_reviewed.json
- cross_site_duplicates_review_progress.json
- cross_site_merge_plan.json
"""

import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import load_json, save_json, load_progress, save_progress
from utils.llm import call_llm, parse_json_response

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DUPLICATES_FILE = DATA_DIR / "cross_site_duplicates.json"
OUTPUT_FILE = DATA_DIR / "cross_site_duplicates_reviewed.json"
PROGRESS_FILE = DATA_DIR / "cross_site_duplicates_review_progress.json"
MERGE_PLAN_FILE = DATA_DIR / "cross_site_merge_plan.json"

LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
MODEL = "qwen/qwen3.5-9b"

MAX_GROUPS = None
DELAY = 0.5
SAVE_EVERY = 1
MAX_RETRIES = 2

SYSTEM_PROMPT = """You are reviewing job vacancy records collected from different Estonian job portals.

Your task is to determine whether records in one candidate duplicate group represent:
1. the same real vacancy posted on multiple sites, or
2. different vacancies that only share the same title and employer.

Be strict and evidence-based.

Treat records as the same real vacancy only if the role and employer match AND the descriptions share strong concrete signals such as:
- same location
- same department / team / unit
- same salary or compensation details
- same responsibilities
- same requirements / qualifications
- same contact person
- same application deadline

Do NOT merge records just because the title and employer are the same.
If the same employer has multiple openings with the same title in different locations, units, teams, or descriptions, they are different vacancies.

Also evaluate record quality. Prefer as canonical:
- cleaner and fuller text
- less OCR noise
- fewer portal artifacts
- more complete salary / location / requirement information

Return ONLY valid JSON. No markdown. No explanation outside JSON."""

USER_PROMPT_TEMPLATE = """Review this duplicate candidate group.

Return a JSON object with this exact top-level structure:
{{
  "group_decision": "all_same_real_vacancy" | "mixed_multiple_vacancies" | "all_different" | "uncertain",
  "summary": "short explanation",
  "clusters": [
    {{
      "cluster_id": "cluster_1",
      "summary": "what this real vacancy is",
      "record_ids": ["source:id", "..."],
      "is_real_vacancy_cluster": true,
      "is_cross_site_duplicate": true,
      "best_record_id": "source:id",
      "best_record_reason": "why this version is best",
      "confidence": "high" | "medium" | "low",
      "shared_signals": ["signal 1", "signal 2"]
    }}
  ],
  "record_evaluations": [
    {{
      "record_id": "source:id",
      "quality_label": "clean_html" | "good_iframe" | "usable_ocr" | "noisy_ocr" | "truncated" | "poor_quality",
      "quality_score": 1,
      "notes": "short note"
    }}
  ],
  "unassigned_record_ids": []
}}

Rules:
- Every input record must appear exactly once either in one cluster or in unassigned_record_ids.
- A cluster should represent one real vacancy only.
- If a group contains two different real vacancies, create two clusters.
- If a record is too unclear to place confidently, put it in unassigned_record_ids.
- Use record IDs exactly as provided.
- quality_score must be an integer from 1 to 5.

Candidate group:
{group_json}
"""


def record_uid(record: dict) -> str:
    return f"{record.get('source')}:{record.get('id')}"


def prepare_group_payload(group: dict) -> dict:
    records = []
    for site_key in ("cv_jobs", "cvkeskus_jobs"):
        for record in group.get(site_key, []):
            records.append(
                {
                    "record_id": record_uid(record),
                    "source": record.get("source"),
                    "id": record.get("id"),
                    "url": record.get("url"),
                    "title": record.get("title"),
                    "company": record.get("company"),
                    "content_type": record.get("content_type"),
                    "full_text_length": len((record.get("full_text") or "").strip()),
                    "full_text": (record.get("full_text") or "").strip(),
                }
            )

    return {
        "match_key": group.get("match_key", {}),
        "record_count": len(records),
        "records": records,
    }


def validate_analysis(analysis: dict, group_payload: dict) -> dict:
    valid_ids = {record["record_id"] for record in group_payload["records"]}
    seen_ids = set()

    clusters = analysis.get("clusters", [])
    if not isinstance(clusters, list):
        raise ValueError("clusters must be a list")

    for cluster in clusters:
        cluster_ids = cluster.get("record_ids", [])
        if not isinstance(cluster_ids, list):
            raise ValueError("cluster record_ids must be a list")
        for rid in cluster_ids:
            if rid not in valid_ids:
                raise ValueError(f"Unknown record_id in clusters: {rid}")
            if rid in seen_ids:
                raise ValueError(f"Duplicate record_id across clusters: {rid}")
            seen_ids.add(rid)
        best_record_id = cluster.get("best_record_id")
        if best_record_id and best_record_id not in cluster_ids:
            raise ValueError(f"best_record_id not inside cluster: {best_record_id}")

    unassigned = analysis.get("unassigned_record_ids", [])
    if not isinstance(unassigned, list):
        raise ValueError("unassigned_record_ids must be a list")
    for rid in unassigned:
        if rid not in valid_ids:
            raise ValueError(f"Unknown record_id in unassigned_record_ids: {rid}")
        if rid in seen_ids:
            raise ValueError(f"Duplicate record_id between cluster and unassigned: {rid}")
        seen_ids.add(rid)

    missing_ids = sorted(valid_ids - seen_ids)
    if missing_ids:
        analysis["unassigned_record_ids"] = sorted(set(unassigned + missing_ids))

    record_evaluations = analysis.get("record_evaluations", [])
    if not isinstance(record_evaluations, list):
        raise ValueError("record_evaluations must be a list")
    eval_ids = set()
    for item in record_evaluations:
        rid = item.get("record_id")
        if rid not in valid_ids:
            raise ValueError(f"Unknown record_id in record_evaluations: {rid}")
        eval_ids.add(rid)
    for missing_id in sorted(valid_ids - eval_ids):
        record_evaluations.append(
            {
                "record_id": missing_id,
                "quality_label": "poor_quality",
                "quality_score": 1,
                "notes": "Missing evaluation from model.",
            }
        )
    analysis["record_evaluations"] = record_evaluations

    if "group_decision" not in analysis:
        raise ValueError("Missing group_decision")
    if "summary" not in analysis:
        analysis["summary"] = ""

    return analysis


def load_existing_results(path: str) -> list[dict]:
    output_path = Path(path)
    if not output_path.exists():
        return []
    data = load_json(path)
    return data if isinstance(data, list) else []


def build_merge_plan(reviewed_groups: list[dict]) -> list[dict]:
    merge_plan = []
    for item in reviewed_groups:
        status = item.get("status")
        group = item.get("group", {})
        analysis = item.get("analysis", {})

        if status != "ok":
            merge_plan.append(
                {
                    "match_key": group.get("match_key", {}),
                    "status": "manual_review",
                    "reason": item.get("error", "Analysis failed"),
                    "clusters": [],
                }
            )
            continue

        group_records = {}
        for site_key in ("cv_jobs", "cvkeskus_jobs"):
            for record in group.get(site_key, []):
                group_records[record_uid(record)] = record

        cluster_entries = []
        for cluster in analysis.get("clusters", []):
            record_ids = cluster.get("record_ids", [])
            sources = {group_records[rid].get("source") for rid in record_ids if rid in group_records}
            best_record_id = cluster.get("best_record_id")
            keep_record = group_records.get(best_record_id) if best_record_id else None

            cluster_entries.append(
                {
                    "cluster_id": cluster.get("cluster_id"),
                    "summary": cluster.get("summary"),
                    "record_ids": record_ids,
                    "sources": sorted(source for source in sources if source),
                    "is_cross_site_duplicate": bool(len(sources) > 1 and len(record_ids) > 1),
                    "best_record_id": best_record_id,
                    "best_record_reason": cluster.get("best_record_reason"),
                    "confidence": cluster.get("confidence"),
                    "keep_record": {
                        "source": keep_record.get("source"),
                        "id": keep_record.get("id"),
                        "url": keep_record.get("url"),
                        "title": keep_record.get("title"),
                        "company": keep_record.get("company"),
                        "content_type": keep_record.get("content_type"),
                    }
                    if keep_record
                    else None,
                    "drop_record_ids": [rid for rid in record_ids if rid != best_record_id],
                }
            )

        merge_plan.append(
            {
                "match_key": group.get("match_key", {}),
                "status": "ok",
                "group_decision": analysis.get("group_decision"),
                "summary": analysis.get("summary"),
                "clusters": cluster_entries,
                "unassigned_record_ids": analysis.get("unassigned_record_ids", []),
            }
        )

    return merge_plan


def main() -> None:
    groups = load_json(DUPLICATES_FILE)
    if not isinstance(groups, list):
        raise ValueError(f"{DUPLICATES_FILE} must contain a JSON list")

    if MAX_GROUPS:
        groups = groups[:MAX_GROUPS]

    reviewed = load_existing_results(OUTPUT_FILE)
    completed_keys = {
        json.dumps(item.get("group", {}).get("match_key", {}), ensure_ascii=False, sort_keys=True)
        for item in reviewed
        if item.get("status") == "ok"
    }
    next_index = load_progress(PROGRESS_FILE)
    processed_since_save = 0

    log.info(f"Loaded {len(groups)} duplicate groups from {DUPLICATES_FILE}")
    if reviewed:
        log.info(f"Resuming with {len(reviewed)} saved review entries")
    if next_index:
        log.info(f"Continuing from index {next_index}")

    for i in range(next_index, len(groups)):
        group = groups[i]
        group_key = json.dumps(group.get("match_key", {}), ensure_ascii=False, sort_keys=True)
        if group_key in completed_keys:
            continue

        match_key = group.get("match_key", {})
        log.info(f"[{i+1}/{len(groups)}] {match_key.get('title')} | {match_key.get('company')}")

        group_payload = prepare_group_payload(group)
        entry = {"group": group, "status": "error"}

        try:
            raw_response = None
            last_error = None
            for attempt in range(MAX_RETRIES + 1):
                try:
                    raw_response = call_llm(
                        group_payload,
                        system_prompt=SYSTEM_PROMPT,
                        user_prompt_template=USER_PROMPT_TEMPLATE,
                        model=MODEL,
                        lm_studio_url=LM_STUDIO_URL,
                        max_tokens=3000,
                    )
                    analysis = parse_json_response(raw_response)
                    analysis = validate_analysis(analysis, group_payload)
                    entry = {
                        "group": group,
                        "status": "ok",
                        "analysis": analysis,
                    }
                    log.info(f"  OK [{analysis.get('group_decision')}] clusters={len(analysis.get('clusters', []))}")
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt == MAX_RETRIES:
                        raise
                    time.sleep(DELAY)
            if entry["status"] != "ok":
                raise last_error
        except Exception as e:
            entry["error"] = str(e)
            if raw_response:
                entry["raw_response"] = raw_response
            log.error(f"Error: {e}")

        reviewed.append(entry)
        processed_since_save += 1
        next_index = i + 1

        if processed_since_save >= SAVE_EVERY:
            save_json(OUTPUT_FILE, reviewed)
            save_progress(PROGRESS_FILE, next_index)
            save_json(MERGE_PLAN_FILE, build_merge_plan(reviewed))
            processed_since_save = 0

        time.sleep(DELAY)

    save_json(OUTPUT_FILE, reviewed)
    save_progress(PROGRESS_FILE, len(groups))
    save_json(MERGE_PLAN_FILE, build_merge_plan(reviewed))

    ok_count = sum(1 for item in reviewed if item.get("status") == "ok")
    error_count = sum(1 for item in reviewed if item.get("status") != "ok")
    log.info(f"Done. Reviewed groups: {len(reviewed)}")
    log.info(f"Successful analyses: {ok_count}")
    log.info(f"Errors: {error_count}")
    log.info(f"Saved detailed reviews to {OUTPUT_FILE}")
    log.info(f"Saved merge plan to {MERGE_PLAN_FILE}")


if __name__ == "__main__":
    main()
