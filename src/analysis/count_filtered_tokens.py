"""
Count tokens for filtered vacancy full_text fields in merged_jobs.json
using the Qwen tokenizer from transformers.
"""

import logging
import sys
from pathlib import Path

from transformers import AutoTokenizer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.io import load_json

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

JOBS_FILE = DATA_DIR / "merged_jobs.json"
MODEL_NAME = "qwen/qwen3.5-9b"
QWEN_CONTEXT_WINDOW = 260_000

FILTER_SOURCE = "cvkeskus"
FILTER_CONTENT_TYPE = "html"


def main() -> None:
    jobs = load_json(JOBS_FILE)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)

    filtered_jobs = [
        job
        for job in jobs
        if job.get("source") == FILTER_SOURCE
        and job.get("content_type") == FILTER_CONTENT_TYPE
    ]

    texts = []
    non_empty_jobs = 0
    empty_jobs = 0

    for job in filtered_jobs:
        full_text = (job.get("full_text") or "").strip()
        if full_text:
            texts.append(full_text)
            non_empty_jobs += 1
        else:
            empty_jobs += 1

    combined_text = "\n\n".join(texts)
    char_count = len(combined_text)
    word_count = len(combined_text.split())
    token_count = len(tokenizer.encode(combined_text, add_special_tokens=False))

    log.info(f"Jobs file: {JOBS_FILE}")
    log.info(f"Model tokenizer: {MODEL_NAME}")
    log.info(f"Filter source: {FILTER_SOURCE}")
    log.info(f"Filter content_type: {FILTER_CONTENT_TYPE}")
    log.info(f"Matching jobs: {len(filtered_jobs)}")
    log.info(f"Jobs with non-empty full_text: {non_empty_jobs}")
    log.info(f"Jobs with empty full_text: {empty_jobs}")
    log.info(f"Total characters in combined full_text: {char_count}")
    log.info(f"Total words in combined full_text: {word_count}")
    log.info(f"Exact token count: {token_count}")

    if token_count <= QWEN_CONTEXT_WINDOW:
        log.info(f"Fits into a {QWEN_CONTEXT_WINDOW}-token window.")
    else:
        overflow = token_count - QWEN_CONTEXT_WINDOW
        log.info(f"Exceeds a {QWEN_CONTEXT_WINDOW}-token window by {overflow} tokens.")


if __name__ == "__main__":
    main()
