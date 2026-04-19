import json

import requests


def call_llm(
    group_payload: dict,
    system_prompt: str,
    user_prompt_template: str,
    model: str,
    lm_studio_url: str,
    max_tokens: int = 3000,
) -> str:
    """Send a group payload to LM Studio and return the raw text response."""
    prompt = user_prompt_template.format(
        group_json=json.dumps(group_payload, ensure_ascii=False, indent=2)
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    resp = requests.post(lm_studio_url, json=payload, timeout=240)
    if not resp.ok:
        raise RuntimeError(f"LM Studio error {resp.status_code}: {resp.text}")
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip()


def parse_json_response(text: str) -> dict:
    """Parse a JSON object from LLM output, tolerating surrounding prose."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(text[start : end + 1])
