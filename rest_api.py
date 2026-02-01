from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

import requests


@dataclass
class PagePagination:
    page_param: str = "page"
    page_size_param: str = "per_page"
    page_size: int = 500
    max_pages: int = 10_000


@dataclass
class IncrementalConfig:
    """
    Simple incremental strategy: pass an `updated_since` style parameter.
    """
    param: str
    from_days_ago: int = 7


class RestApiClient:
    def __init__(self, base_url: str, headers: Dict[str, str], timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.headers = headers
        self.timeout = timeout

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, headers=self.headers, params=params or {}, timeout=self.timeout)
        resp.raise_for_status()
        return resp


def _parse_items(response_json: Any) -> list[dict]:
    """
    Accept common API shapes:
      - list[dict]
      - { "data": [...] }
      - { "items": [...] }
    """
    if isinstance(response_json, list):
        return response_json
    if isinstance(response_json, dict):
        for k in ("data", "items", "results"):
            if k in response_json and isinstance(response_json[k], list):
                return response_json[k]
    raise ValueError("Unsupported response shape for items")


def iter_paginated(
    client: RestApiClient,
    path: str,
    pagination: PagePagination,
    incremental: Optional[IncrementalConfig] = None,
) -> Iterable[dict]:
    page = 1
    base_params: Dict[str, Any] = {
        pagination.page_param: page,
        pagination.page_size_param: pagination.page_size,
    }

    if incremental:
        since = (dt.datetime.utcnow() - dt.timedelta(days=incremental.from_days_ago)).date().isoformat()
        base_params[incremental.param] = since

    while page <= pagination.max_pages:
        params = dict(base_params)
        params[pagination.page_param] = page

        resp = client.get(path, params=params)
        payload = resp.json()
        items = _parse_items(payload)

        if not items:
            break

        for item in items:
            yield item

        # naive stop condition: if fewer than page_size items, assume last page
        if len(items) < pagination.page_size:
            break

        page += 1
        time.sleep(0.2)  # gentle rate-limit


def to_jsonl(records: Iterable[dict]) -> str:
    buf = []
    for r in records:
        buf.append(json.dumps(r, ensure_ascii=False))
    return "\n".join(buf) + ("\n" if buf else "")
