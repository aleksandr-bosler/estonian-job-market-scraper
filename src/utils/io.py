import json
import re
from pathlib import Path


def load_json(path: str):
    with Path(path).open(encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data) -> None:
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_progress(path: str) -> int:
    """Load next_index from a progress file. Returns 0 if file does not exist."""
    progress_path = Path(path)
    if not progress_path.exists():
        return 0
    with progress_path.open(encoding="utf-8") as f:
        data = json.load(f)
    return int(data.get("next_index", 0))


def save_progress(path: str, next_index: int) -> None:
    save_json(path, {"next_index": next_index})


def safe_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()


def load_results(output_file) -> list[dict]:
    path = Path(output_file)
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def load_done_ids(results: list[dict]) -> set[str]:
    done_ids = set()
    for item in results:
        job_id = item.get("id") or item.get("job_id")
        if job_id:
            done_ids.add(str(job_id))
    return done_ids


def flush_state(results: list[dict], output_file, progress_file, next_index: int) -> None:
    save_json(output_file, results)
    save_progress(progress_file, next_index)
