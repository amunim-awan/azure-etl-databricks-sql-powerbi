from __future__ import annotations

import os
import json
import requests
from typing import List

from src.qc.checks import QCResult


def post_slack(webhook_url: str, text: str) -> None:
    payload = {"text": text}
    resp = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=30)
    resp.raise_for_status()


def format_qc_results(results: List[QCResult]) -> str:
    lines = []
    for r in results:
        icon = "✅" if r.passed else "❌"
        lines.append(f"{icon} {r.check_name}: {r.detail}")
    return "\n".join(lines)
