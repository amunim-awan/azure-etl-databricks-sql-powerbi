from __future__ import annotations

import os
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class SecretProvider:
    """
    Fetch secrets from Azure Key Vault. Falls back to environment variables if Key Vault is not configured.

    Recommended pattern:
    - Store API keys, DB credentials, and OAuth secrets in Key Vault.
    - Use DefaultAzureCredential (managed identity in Azure, or service principal locally).
    """

    def __init__(self, keyvault_url: Optional[str]):
        self.keyvault_url = keyvault_url
        self._client = None

        if keyvault_url:
            credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
            self._client = SecretClient(vault_url=keyvault_url, credential=credential)

    def get_secret(self, secret_name: str, env_fallback: Optional[str] = None) -> str:
        if not secret_name:
            raise ValueError("secret_name is required")

        # Try Key Vault
        if self._client:
            secret = self._client.get_secret(secret_name)
            return secret.value

        # Fallback to env
        if env_fallback and os.getenv(env_fallback):
            return os.environ[env_fallback]

        raise RuntimeError(
            f"Key Vault not configured and env fallback missing for secret '{secret_name}'. "
            f"Set AZURE_KEYVAULT_URL or provide {env_fallback}."
        )
