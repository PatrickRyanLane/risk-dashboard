#!/usr/bin/env python3
"""
Helpers for LLM gating/prompting (no API calls here).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import os
import sys

import requests


def is_uncertain(label: str, is_finance: bool, is_forced: bool, compound: float | None,
                 cleaned_title: str, raw_title: str) -> Tuple[bool, str]:
    """
    Mark items for LLM review when sentiment is negative/neutral and low confidence.
    """
    if is_finance or is_forced:
        return False, ""
    if label not in ("negative", "neutral"):
        return False, ""
    title = cleaned_title or raw_title or ""
    word_count = len([w for w in title.split() if w])
    if compound is None:
        if word_count <= 6:
            return True, "short_title"
        return False, ""
    if -0.2 < compound < 0.2:
        return True, "low_compound"
    if word_count <= 6:
        return True, "short_title"
    return False, ""


def uncertainty_priority(compound: float | None, title: str) -> float:
    """
    Higher score = more uncertain. Emphasize near-zero compound + short titles.
    """
    score = 0.0
    if compound is None:
        score += 1.0
    else:
        score += 1.0 - min(abs(compound), 1.0)
    words = len([w for w in (title or "").split() if w])
    if words <= 6:
        score += 0.5
    return score


def build_risk_prompt(entity_type: str, entity_name: str, title: str,
                      snippet: str = "", source: str = "", url: str = "") -> dict:
    """
    Minimal prompt template for crisis/risk classification.
    """
    system = (
        "You are an online reputation risk analyst. "
        "Return both sentiment and risk. "
        "Sentiment must be: positive, neutral, or negative. "
        "Risk must be: crisis_risk, routine_financial, or not_risk. "
        "Also classify control_class as controlled or uncontrolled when you can infer it. "
        "Respond with compact JSON: {sentiment_label, risk_label, severity, reason, control_class}."
    )
    user = (
        f"Entity: {entity_type} = {entity_name}\n"
        f"Title: {title}\n"
        f"Snippet: {snippet}\n"
        f"Source: {source}\n"
        f"URL: {url}\n"
        "Return JSON only."
    )
    return {"system": system, "user": user}


def build_summary_prompt(entity_type: str, entity_name: str, headlines: list[str]) -> dict:
    system = (
        "You summarize brand/CEO crisis context for internal users. "
        "Write 1-2 concise sentences, neutral tone, no speculation."
    )
    items = "\n".join(f"- {h}" for h in headlines if h)
    user = (
        f"Entity: {entity_type} = {entity_name}\n"
        f"Headlines:\n{items}\n"
        "Return summary only."
    )
    return {"system": system, "user": user}


def _parse_json_from_text(text: str) -> dict:
    text = text.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return {}
    return {}

def _log_llm_error(prefix: str, status_code: int, body: str) -> None:
    if os.getenv("LLM_DEBUG", "0") != "1":
        return
    snippet = body.strip()
    if len(snippet) > 1000:
        snippet = snippet[:1000] + "…"
    print(f"{prefix} HTTP {status_code}: {snippet}", file=sys.stderr)


def call_llm_openai(prompt: dict, api_key: str, model: str,
                    timeout: int = 20) -> dict:
    """
    Call OpenAI chat completions API and return parsed JSON.
    """
    if not api_key:
        return {}
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": prompt["system"]},
            {"role": "user", "content": prompt["user"]},
        ],
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if resp.status_code != 200:
        _log_llm_error("OpenAI error", resp.status_code, resp.text)
        return {}
    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except Exception:
        return {}
    return _parse_json_from_text(content)


def call_llm_openai_text(prompt: dict, api_key: str, model: str,
                         timeout: int = 20) -> str:
    if not api_key:
        return ""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": prompt["system"]},
            {"role": "user", "content": prompt["user"]},
        ],
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if resp.status_code != 200:
        _log_llm_error("OpenAI error", resp.status_code, resp.text)
        return ""
    data = resp.json()
    try:
        return str(data["choices"][0]["message"]["content"]).strip()
    except Exception:
        return ""


def call_llm_gemini(prompt: dict, api_key: str, model: str,
                    timeout: int = 20) -> dict:
    if not api_key:
        return {}
    if not model:
        model = "gemini-1.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    params = {"key": api_key}
    merged = f"{prompt['system']}\n\n{prompt['user']}"
    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": merged}]}
        ],
        "generationConfig": {
            "temperature": 0.0
        }
    }
    resp = requests.post(url, params=params, json=payload, timeout=timeout)
    if resp.status_code != 200:
        _log_llm_error("Gemini error", resp.status_code, resp.text)
        return {}
    data = resp.json()
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        return {}
    return _parse_json_from_text(text)


def call_llm_gemini_text(prompt: dict, api_key: str, model: str,
                         timeout: int = 20) -> str:
    if not api_key:
        return ""
    if not model:
        model = "gemini-1.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    params = {"key": api_key}
    merged = f"{prompt['system']}\n\n{prompt['user']}"
    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": merged}]}
        ],
        "generationConfig": {
            "temperature": 0.2
        }
    }
    resp = requests.post(url, params=params, json=payload, timeout=timeout)
    if resp.status_code != 200:
        _log_llm_error("Gemini error", resp.status_code, resp.text)
        return ""
    data = resp.json()
    try:
        return str(data["candidates"][0]["content"]["parts"][0]["text"]).strip()
    except Exception:
        return ""


def call_llm_json(prompt: dict, api_key: str, model: str,
                  provider: str | None = None) -> dict:
    provider = (provider or os.getenv("LLM_PROVIDER", "openai")).lower()
    if provider == "gemini":
        return call_llm_gemini(prompt, api_key, model)
    return call_llm_openai(prompt, api_key, model)


def call_llm_text(prompt: dict, api_key: str, model: str,
                  provider: str | None = None) -> str:
    provider = (provider or os.getenv("LLM_PROVIDER", "openai")).lower()
    if provider == "gemini":
        return call_llm_gemini_text(prompt, api_key, model)
    return call_llm_openai_text(prompt, api_key, model)


def load_json_cache(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json_cache(path: str, cache: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, ensure_ascii=True, indent=2), encoding="utf-8")
