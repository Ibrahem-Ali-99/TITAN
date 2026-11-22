"""Load .env without overriding variables already set in the process."""

import os
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_cwd_env() -> None:
    """Load `.env` from the repository root when the process was started there."""
    root = Path.cwd()
    if (root / "docker-compose.yml").is_file():
        load_env_file(root / ".env")
