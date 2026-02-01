from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass
class QuickBooksAuthConfig:
    token_url: str
    client_id: str
    client_secret: str
    refresh_token: str


class QuickBooksClient:
    """
    Minimal QuickBooks Online connector.

    Notes:
    - QuickBooks uses OAuth2. In production, store secrets in Key Vault.
    - This connector refreshes an access token using a refresh token.
    - Data is fetched using QBO query endpoint (SQL-like queries).
    """

    def __init__(self, auth: QuickBooksAuthConfig, company_id: str, env: str = "production", timeout: int = 60):
        self.auth = auth
        self.company_id = company_id
        self.env = env
        self.timeout = timeout
        self._access_token: Optional[str] = None

    @property
    def base_url(self) -> str:
        # QuickBooks API base (production vs sandbox differ in hostname)
        host = "quickbooks.api.intuit.com" if self.env == "production" else "sandbox-quickbooks.api.intuit.com"
        return f"https://{host}/v3/company/{self.company_id}"

    def refresh_access_token(self) -> str:
        basic = base64.b64encode(f"{self.auth.client_id}:{self.auth.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.auth.refresh_token,
        }
        resp = requests.post(self.auth.token_url, headers=headers, data=data, timeout=self.timeout)
        resp.raise_for_status()
        token_json = resp.json()
        self._access_token = token_json["access_token"]
        # QBO might return a new refresh_token; handle if you want rotation.
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        if not self._access_token:
            self.refresh_access_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }

    def query(self, query: str, minorversion: int = 75) -> dict:
        url = f"{self.base_url}/query"
        params = {"query": query, "minorversion": minorversion}
        resp = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)

        # If token expired, refresh once and retry
        if resp.status_code == 401:
            self.refresh_access_token()
            resp = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)

        resp.raise_for_status()
        return resp.json()
