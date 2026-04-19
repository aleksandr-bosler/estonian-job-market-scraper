"""
OCR images for jobs with content_type='unknown' using LM Studio (Qwen3.5-9B).
Reads cv_jobs.json, fills full_text for image-only jobs, saves back to cv_jobs.json.
"""

import base64
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

JOBS_FILE = DATA_DIR / "cv_jobs.json"
IMAGES_DIR = DATA_DIR / "images" / "cv"
LM_STUDIO_URL = "http://localhost:1234/v1/chat/completions"
MODEL = "qwen/qwen3.5-9b"
DELAY = 1.0
MAX_JOBS = None     # None to process all

PROMPT = """Extract all text from this image. Return only the text, no explanations."""


def image_to_base64(filepath: str) -> str:
    with open(filepath, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def ocr_image(image_path: str) -> str:
    b64 = image_to_base64(image_path)
    ext = image_path.rsplit(".", 1)[-1].lower()
    media_type = f"image/{ext}" if ext in ("jpg", "jpeg", "png", "webp") else "image/jpeg"
    if ext == "jpg":
        media_type = "image/jpeg"

    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{media_type};base64,{b64}"},
                    },
                    {
                        "type": "text",
                        "text": PROMPT,
                    },
                ],
            }
        ],
        "temperature": 0.1,
        "max_tokens": 4000,
    }

    resp = requests.post(LM_STUDIO_URL, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip()


def main():
    with open(JOBS_FILE, encoding="utf-8") as f:
        jobs = json.load(f)

    ocr_jobs = [
        j
        for j in jobs
        if j.get("content_type") in {"image_only", "iframe_image_only"}
        and not (j.get("full_text") or "").strip()
        and j.get("images")
    ]
    log.info(f"Found {len(ocr_jobs)} image-only jobs with empty full_text")

    if MAX_JOBS:
        ocr_jobs = ocr_jobs[:MAX_JOBS]
        log.info(f"Processing first {MAX_JOBS} (change MAX_JOBS=None for all)")

    jobs_by_id = {j["id"]: j for j in jobs}

    processed = 0
    errors = 0

    for i, job in enumerate(ocr_jobs):
        job_id = job["id"]
        images = job["images"]
        log.info(f"[{i+1}/{len(ocr_jobs)}] {job_id} - {job['title']}")

        texts = []
        for filename in images:
            filepath = os.path.join(IMAGES_DIR, filename)
            if not os.path.exists(filepath):
                log.warning(f"Image not found: {filepath}")
                continue
            try:
                text = ocr_image(filepath)
                if text:
                    texts.append(text)
            except Exception as e:
                log.error(f"OCR error: {e}")
                errors += 1

        if texts:
            full_text = "\n\n".join(texts)
            jobs_by_id[job_id]["full_text"] = full_text
            jobs_by_id[job_id]["content_type"] = "ocr"
            processed += 1
            log.info(f"OK text_len={len(full_text)}")
        else:
            log.warning("FAILED - no text extracted")

        with open(JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(jobs_by_id.values()), f, ensure_ascii=False, indent=2)

        time.sleep(DELAY)

    remaining = sum(
        1
        for j in jobs_by_id.values()
        if j.get("content_type") in {"image_only", "iframe_image_only"}
        and not (j.get("full_text") or "").strip()
    )
    log.info(f"Done. Processed: {processed}, Errors: {errors}")
    log.info(f"Remaining image-only without text: {remaining}")


if __name__ == "__main__":
    main()
