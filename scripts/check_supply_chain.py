"""Fail closed on mutable CI actions and protected container references."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
COMPOSE = ROOT / "docker-compose.yml"
DOCKERFILES = tuple((ROOT / "infrastructure" / "docker").glob("*.Dockerfile"))

ACTION_RE = re.compile(r"uses:\s*[^\s#]+@([^\s#]+)")
IMAGE_RE = re.compile(r"^\s*image:\s*([^\s#]+)", re.MULTILINE)
FROM_RE = re.compile(r"^\s*(?:FROM|COPY\s+--from=)([^\s]+)", re.MULTILINE)


def main() -> int:
    failures: list[str] = []
    for workflow in sorted(WORKFLOWS.glob("*.yml")):
        for reference in ACTION_RE.findall(workflow.read_text(encoding="utf-8")):
            if not re.fullmatch(r"[0-9a-f]{40}", reference):
                failures.append(f"{workflow.relative_to(ROOT)}: mutable action {reference}")

    compose_text = COMPOSE.read_text(encoding="utf-8")
    for image in IMAGE_RE.findall(compose_text):
        if "TITAN_SPARK_IMAGE" in image or image.startswith("titan-spark:"):
            continue  # Local build tag; hosted CI resolves/pins the GHCR digest.
        if "@sha256:" not in image:
            failures.append(f"docker-compose.yml: mutable image {image}")

    for dockerfile in DOCKERFILES:
        for reference in FROM_RE.findall(dockerfile.read_text(encoding="utf-8")):
            if reference.startswith("${") or "@sha256:" not in reference:
                failures.append(f"{dockerfile.relative_to(ROOT)}: mutable base {reference}")

    if failures:
        print("\n".join(failures))
        return 1
    print("Supply-chain references are immutable except the documented local Spark build tag.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
