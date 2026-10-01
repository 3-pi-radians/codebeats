"""Load config/settings.yaml and resolve secrets from environment variables."""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS = REPO_ROOT / "config" / "settings.yaml"


def load_settings(path: Path | str = DEFAULT_SETTINGS) -> dict:
    load_dotenv(REPO_ROOT / ".env")  # no-op if .env is absent
    with open(path) as f:
        return yaml.safe_load(f)


def secret(env_name: str) -> str:
    """Return a secret from the environment. The error names the variable, never the value."""
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable {env_name} is not set (see .env.example)")
    return value
