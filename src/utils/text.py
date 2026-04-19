import re
import unicodedata


def normalize_text(value: str) -> str:
    """Normalize text for fuzzy matching: NFKC, casefold, collapse whitespace."""
    value = unicodedata.normalize("NFKC", value or "")
    value = value.casefold().strip()
    value = re.sub(r"\s+", " ", value)
    return value
