"""Load a checkout `.env` without overriding the process environment."""

import os
from pathlib import Path


def load_cwd_env() -> None:
    root = Path.cwd()
    path = root / ".env"
    if not (root / "docker-compose.yml").is_file() or not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
