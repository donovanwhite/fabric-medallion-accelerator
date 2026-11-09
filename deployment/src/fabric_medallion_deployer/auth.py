"""Authentication helpers for Microsoft Fabric REST calls."""

from __future__ import annotations

import shutil
import subprocess


def azure_cli_token() -> str:
    """Acquire a Fabric token from the active Azure CLI identity."""
    executable = shutil.which("az") or shutil.which("az.cmd")
    if not executable:
        raise RuntimeError(
            "Azure CLI is required. Install it, then run "
            "'az login --allow-no-subscriptions'."
        )
    result = subprocess.run(
        [
            executable,
            "account",
            "get-access-token",
            "--resource",
            "https://api.fabric.microsoft.com",
            "--query",
            "accessToken",
            "--output",
            "tsv",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    token = result.stdout.strip()
    if not token:
        raise RuntimeError("Azure CLI returned an empty Fabric access token")
    return token
