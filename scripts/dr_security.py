"""Platform-aware protections for local disaster-recovery artifacts."""

from __future__ import annotations

import os
from pathlib import Path


def secure_directory(path: Path) -> None:
    if os.name != "nt":
        path.chmod(0o700)


def secure_file(path: Path) -> None:
    if os.name != "nt":
        path.chmod(0o600)


def secure_tree(root: Path) -> None:
    """Restrict an artifact tree on POSIX; Windows uses host ACL policy."""
    if os.name == "nt":
        return
    for path in root.rglob("*"):
        if path.is_dir():
            secure_directory(path)
        elif path.is_file():
            secure_file(path)
    secure_directory(root)
