from __future__ import annotations

import re
from typing import Iterable


RELATIVE_DATE_RE = re.compile(
    r"\b\d+\s+(?:sec|secs|second|seconds|min|mins|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\b",
    re.IGNORECASE,
)
SHORT_RELATIVE_DATE_RE = re.compile(r"^\d+\s*[smhdwy]$", re.IGNORECASE)
ABSOLUTE_DATE_RE = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4})\b",
    re.IGNORECASE,
)


def _clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text)


def _looks_like_date_text(text: str) -> bool:
    if not text:
        return False
    return bool(
        RELATIVE_DATE_RE.search(text)
        or SHORT_RELATIVE_DATE_RE.fullmatch(text)
        or ABSOLUTE_DATE_RE.search(text)
    )


def _first_date_text(values) -> str:
    if isinstance(values, str):
        text = _clean_text(values)
        return text if _looks_like_date_text(text) else ""
    if not isinstance(values, Iterable):
        return ""
    for value in values:
        text = _clean_text(value)
        if _looks_like_date_text(text):
            return text
    return ""


def extract_serp_date_text(item: dict) -> str:
    if not isinstance(item, dict):
        return ""

    for key in ("date", "published_date", "published_at", "published", "time_ago", "upload_date"):
        text = _clean_text(item.get(key))
        if text:
            return text

    rich_snippet = item.get("rich_snippet") or {}
    top = rich_snippet.get("top") if isinstance(rich_snippet, dict) else {}

    for container in (item, top):
        if not isinstance(container, dict):
            continue
        for key in ("detected_extensions", "extensions"):
            text = _first_date_text(container.get(key))
            if text:
                return text

    return ""
