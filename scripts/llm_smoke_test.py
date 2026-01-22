#!/usr/bin/env python3
"""
Simple smoke test for LLM provider credentials/config.
"""
from __future__ import annotations

import os
import sys

from llm_utils import build_risk_prompt, build_summary_prompt, call_llm_json, call_llm_text


def main() -> int:
    api_key = os.getenv("LLM_API_KEY", "")
    provider = os.getenv("LLM_PROVIDER", "openai")
    model = os.getenv("LLM_MODEL", "")

    if not api_key:
        print("Missing LLM_API_KEY.", file=sys.stderr)
        return 1

    prompt = build_risk_prompt(
        entity_type="company",
        entity_name="Acme Corp",
        title="Acme CEO resigns amid investigation",
        snippet="Board cites compliance concerns.",
        source="Example News",
        url="https://example.com/acme-resigns",
    )
    resp = call_llm_json(prompt, api_key, model, provider=provider)
    if not resp:
        print("LLM JSON call failed or returned empty response.", file=sys.stderr)
        return 2
    print("LLM JSON response:", resp)

    summary_prompt = build_summary_prompt(
        entity_type="company",
        entity_name="Acme Corp",
        headlines=[
            "Acme CEO resigns amid investigation",
            "Regulators probe Acme accounting practices",
        ],
    )
    summary = call_llm_text(summary_prompt, api_key, model, provider=provider)
    if not summary:
        print("LLM summary call failed or returned empty response.", file=sys.stderr)
        return 3
    print("LLM summary response:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
