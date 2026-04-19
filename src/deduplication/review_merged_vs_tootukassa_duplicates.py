"""
Review duplicate candidates between merged_jobs and tootukassa with a local Qwen
model via LM Studio.

Goal:
- decide whether records describe the same real vacancy or not

Outputs:
- merged_vs_tootukassa_duplicates_reviewed.json
- merged_vs_tootukassa_duplicates_review_progress.json
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

DUPLICATES_FILE = DATA_DIR / "merged_vs_tootukassa_duplicates.json"
OUTPUT_FILE = DATA_DIR / "merged_vs_tootukassa_duplicates_reviewed.json"
PROGRESS_FILE = DATA_DIR / "merged_vs_tootukassa_duplicates_review_progress.json"

LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
MODEL = "qwen/qwen3.5-9b"

MAX_GROUPS = None
DELAY = 0.5
SAVE_EVERY = 1
MAX_RETRIES = 2

SYSTEM_PROMPT = """You compare job vacancy records from different Estonian job portals.

Your task is to decide whether records in one candidate group describe:
1. the same real vacancy, posted on different sites, or
2. different vacancies that only share a similar title and employer.

Be strict and evidence-based.

Treat records as the same real vacancy only if the role and employer match AND the
descriptions share strong concrete signals such as:
- same location
- same department / team / unit
- same salary or compensation details
- same responsibilities
- same requirements / qualifications
- same contact person
- same application deadline

Do NOT mark them as different only because one description is shorter or less detailed.
Do NOT mark them as different only because one source is structured and the other is plain text.

Return ONLY valid JSON. No markdown. No explanation outside JSON."""

USER_PROMPT_TEMPLATE = """Review this duplicate candidate group.

Return a JSON object with this exact structure:
{{
  "group_decision": "same_real_vacancy" | "different_vacancies" | "uncertain",
  "summary": "short explanation",
  "pair_assessments": [
    {{
      "merged_record_id": "source:id",
      "tootukassa_record_id": "tootukassa:id",
      "decision": "same_real_vacancy" | "different_vacancies" | "uncertain",
      "confidence": "high" | "medium" | "low",
      "reason": "short explanation",
      "shared_signals": ["signal 1", "signal 2"]
    }}
  ]
}}

Rules:
- Assess each merged record against each tootukassa record in the group.
- Be robust to wording differences, OCR noise, and one source being less detailed.
- If there is not enough evidence either way, use "uncertain".
- Use record IDs exactly as provided.

Candidate group:
{group_json}
"""


def record_uid(record: dict) -> str:
    return f"{record.get('source')}:{record.get('id')}"


def build_tootukassa_text(record: dict) -> str:
    excerpt = record.get("raw_excerpt") or {}
    sections = []

    mapping = (
        ("Ametinimetuse täpsustus", excerpt.get("ametinimetusTapsustus")),
        ("Tööülesanded", excerpt.get("tooylesanded")),
        ("Omalt poolt pakume", excerpt.get("omaltPooltPakume")),
        ("Nõuded", excerpt.get("nouded")),
    )

    for label, value in mapping:
        text = (value or "").strip()
        if text:
            sections.append(f"{label}\n{text}")

    return "\n\n".join(sections).strip()


def prepare_group_payload(group: dict) -> dict:
    merged_records = []
    for record in group.get("merged_jobs", []):
        merged_records.append(
            {
                "record_id": record_uid(record),
                "source": record.get("source"),
                "id": record.get("id"),
                "url": record.get("url"),
                "title": record.get("title"),
                "company": record.get("company"),
                "content_type": record.get("content_type"),
                "full_text": (record.get("full_text") or "").strip(),
            }
        )

    tootukassa_records = []
    for record in group.get("tootukassa_jobs", []):
        tootukassa_records.append(
            {
                "record_id": record_uid(record),
                "source": record.get("source"),
                "id": record.get("id"),
                "url": record.get("url"),
                "title": record.get("title"),
                "company": record.get("company"),
                "content_type": record.get("content_type"),
                "structured_text": build_tootukassa_text(record),
                "raw_excerpt": record.get("raw_excerpt") or {},
            }
        )

    return {
        "match_key": group.get("match_key", {}),
        "merged_records": merged_records,
        "tootukassa_records": tootukassa_records,
    }


def validate_analysis(analysis: dict, group_payload: dict) -> dict:
    merged_ids = {record["record_id"] for record in group_payload["merged_records"]}
    tootukassa_ids = {record["record_id"] for record in group_payload["tootukassa_records"]}
    expected_pairs = {(m, t) for m in merged_ids for t in tootukassa_ids}

    pair_assessments = analysis.get("pair_assessments", [])
    if not isinstance(pair_assessments, list):
        raise ValueError("pair_assessments must be a list")

    seen_pairs = set()
    for item in pair_assessments:
        merged_id = item.get("merged_record_id")
        tootukassa_id = item.get("tootukassa_record_id")
        pair = (merged_id, tootukassa_id)

        if merged_id not in merged_ids:
            raise ValueError(f"Unknown merged_record_id: {merged_id}")
        if tootukassa_id not in tootukassa_ids:
            raise ValueError(f"Unknown tootukassa_record_id: {tootukassa_id}")
        if pair in seen_pairs:
            raise ValueError(f"Duplicate pair assessment: {pair}")
        seen_pairs.add(pair)

    missing_pairs = expected_pairs - seen_pairs
    for merged_id, tootukassa_id in sorted(missing_pairs):
        pair_assessments.append(
            {
                "merged_record_id": merged_id,
                "tootukassa_record_id": tootukassa_id,
                "decision": "uncertain",
                "confidence": "low",
                "reason": "Missing pair assessment from model.",
                "shared_signals": [],
            }
        )
    analysis["pair_assessments"] = pair_assessments

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
        raw_response = None

        try:
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
                    log.info(f"  OK [{analysis.get('group_decision')}]")
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
            processed_since_save = 0

        time.sleep(DELAY)

    save_json(OUTPUT_FILE, reviewed)
    save_progress(PROGRESS_FILE, len(groups))

    ok_count = sum(1 for item in reviewed if item.get("status") == "ok")
    error_count = sum(1 for item in reviewed if item.get("status") != "ok")
    log.info(f"Done. Reviewed groups: {len(reviewed)}")
    log.info(f"Successful analyses: {ok_count}")
    log.info(f"Errors: {error_count}")
    log.info(f"Saved detailed reviews to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
