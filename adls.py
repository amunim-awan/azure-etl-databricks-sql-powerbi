from __future__ import annotations

import io
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class ADLSClient:
    """
    Simple wrapper for Azure Blob Storage / ADLS Gen2 container access.

    We write:
      bronze/{source}/{endpoint}/run_date=YYYY-MM-DD/data.jsonl
      silver/{domain}/{table}/run_date=YYYY-MM-DD/part-*.parquet
      gold/{domain}/{table}/run_date=YYYY-MM-DD/part-*.parquet
    """

    def __init__(self, account_url: str):
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        self.client = BlobServiceClient(account_url=account_url, credential=credential)

    def upload_text(self, container: str, blob_path: str, text: str, overwrite: bool = True) -> None:
        blob = self.client.get_blob_client(container=container, blob=blob_path)
        blob.upload_blob(text.encode("utf-8"), overwrite=overwrite)

    def download_text(self, container: str, blob_path: str) -> str:
        blob = self.client.get_blob_client(container=container, blob=blob_path)
        return blob.download_blob().readall().decode("utf-8")

    def upload_bytes(self, container: str, blob_path: str, data: bytes, overwrite: bool = True) -> None:
        blob = self.client.get_blob_client(container=container, blob=blob_path)
        blob.upload_blob(data, overwrite=overwrite)

    def list_blobs(self, container: str, prefix: str):
        cont = self.client.get_container_client(container)
        return [b.name for b in cont.list_blobs(name_starts_with=prefix)]
