"""Fail when a tracked file contains a private key or a cloud access-key prefix."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = {".git", ".venv", "node_modules", "benchmarks"}
MARKERS = ("BEGIN PRIVATE KEY", "BEGIN RSA PRIVATE KEY", "AKIA")


def main() -> None:
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in SKIP for part in path.parts):
            continue
        if path.name == "scan_secrets.py":
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".parquet"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for marker in MARKERS:
            if marker in text:
                raise SystemExit(f"{path} contains {marker}")


if __name__ == "__main__":
    main()
