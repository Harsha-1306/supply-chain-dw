"""Trigger a Power BI semantic-model refresh after the warehouse load (REST API, service principal)."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
REFRESH_URL = "https://api.powerbi.com/v1.0/myorg/groups/{group}/datasets/{dataset}/refreshes"
SCOPE = "https://analysis.windows.net/powerbi/api/.default"
REQUIRED = ("PBI_TENANT_ID", "PBI_CLIENT_ID", "PBI_CLIENT_SECRET", "PBI_WORKSPACE_ID", "PBI_DATASET_ID")


def configured() -> bool:
    return all(os.getenv(k) for k in REQUIRED)


def _post(url: str, data: bytes, headers: dict[str, str]) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - fixed https endpoints
        return resp.status, resp.read()


def trigger_refresh() -> int:
    """Queue an asynchronous refresh; returns the HTTP status (202 = accepted)."""
    env = {k: os.environ[k] for k in REQUIRED}
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials", "client_id": env["PBI_CLIENT_ID"],
        "client_secret": env["PBI_CLIENT_SECRET"], "scope": SCOPE,
    }).encode()
    _, token_raw = _post(TOKEN_URL.format(tenant=env["PBI_TENANT_ID"]), body,
                         {"Content-Type": "application/x-www-form-urlencoded"})
    token = json.loads(token_raw)["access_token"]
    status, _ = _post(
        REFRESH_URL.format(group=env["PBI_WORKSPACE_ID"], dataset=env["PBI_DATASET_ID"]),
        json.dumps({"notifyOption": "MailOnFailure"}).encode(),
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    return status
