"""Live Kafka recovery and an idempotent warehouse reconcile.

Stopping Kafka must not change the published warehouse. The broker is started
again before the script exits. A browser was not used.
"""

import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    before = _json(base, "/api/v1/warehouse/status")
    if not before.get("ready"):
        raise SystemExit(f"warehouse was not ready before the fault: {before.get('reason')}")
    pointer = before["warehouse_publication_id"]
    revenue = before["accepted_revenue"]
    try:
        subprocess.run(["docker", "compose", "stop", "kafka"], cwd=ROOT, check=True)
        during = _json(base, "/api/v1/warehouse/status")
        if not during.get("ready"):
            raise SystemExit(f"warehouse became unready while Kafka was stopped: {during}")
        if during["warehouse_publication_id"] != pointer or during["accepted_revenue"] != revenue:
            raise SystemExit("Kafka stop changed the published warehouse")
    finally:
        subprocess.run(["docker", "compose", "start", "kafka"], cwd=ROOT, check=True)
    _wait_kafka()
    _wait_json(base, "/health/ready")
    after = _json(base, "/api/v1/warehouse/status")
    if after["warehouse_publication_id"] != pointer or after["accepted_revenue"] != revenue:
        raise SystemExit("Kafka restart changed the published warehouse")
    if not after.get("ready"):
        raise SystemExit(f"warehouse was not ready after Kafka restarted: {after.get('reason')}")
    completed = _python("titan_warehouse", "--reconcile")
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-2000:] or completed.stdout[-2000:])
    if "matches the newest verified publication" not in completed.stdout:
        raise SystemExit(completed.stdout[-1000:])
    final = _json(base, "/api/v1/warehouse/status")
    if final["warehouse_publication_id"] != pointer or final["accepted_revenue"] != revenue:
        raise SystemExit("reconcile changed a verified publication")
    print(
        json.dumps(
            {
                "kafka": "stopped and started",
                "warehouse_ready_during_outage": True,
                "publication": pointer,
                "revenue": revenue,
                "reconcile": completed.stdout.strip(),
            },
            indent=2,
        )
    )


def _wait_kafka() -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        completed = subprocess.run(
            ["docker", "compose", "ps", "--status", "running", "kafka"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if "kafka" in completed.stdout:
            time.sleep(5)
            return
        time.sleep(2)
    raise SystemExit("kafka did not return")


def _python(module: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "pipelines" / "warehouse" / "src"),
            str(ROOT / "pipelines" / "quality" / "src"),
            str(ROOT / "pipelines" / "ml" / "src"),
            str(ROOT / "pipelines" / "dagster" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    env.setdefault("CLICKHOUSE_HOST", "localhost")
    return subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-m", module, *args],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _wait_json(base: str, path: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            _json(base, path)
            return
        except Exception:
            time.sleep(2)
    raise SystemExit(f"{path} did not recover")


def _json(base: str, path: str) -> dict[str, Any]:
    with urllib.request.urlopen(base + path, timeout=20) as response:
        body = json.loads(response.read().decode())
    if not isinstance(body, dict):
        raise SystemExit(f"{path} did not return an object")
    return body


if __name__ == "__main__":
    main()
