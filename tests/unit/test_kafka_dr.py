from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.kafka_dr import RECORDS, verify_backup


def _backup(tmp_path: Path) -> Path:
    directory = tmp_path / "backup"
    directory.mkdir()
    payload = (json.dumps({"topic": "dr", "partition": 0, "offset": 0}) + "\n").encode()
    (directory / RECORDS).write_bytes(payload)
    import hashlib

    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "records_bytes": len(payload),
                "records_sha256": hashlib.sha256(payload).hexdigest(),
                "record_count": 1,
            }
        ),
        encoding="utf-8",
    )
    return directory


def test_kafka_backup_manifest_verifies(tmp_path: Path) -> None:
    assert verify_backup(_backup(tmp_path))["record_count"] == 1


@pytest.mark.parametrize("mutation", ["append", "remove"])
def test_kafka_backup_rejects_corrupt_or_incomplete_payload(tmp_path: Path, mutation: str) -> None:
    directory = _backup(tmp_path)
    if mutation == "append":
        (directory / RECORDS).write_bytes((directory / RECORDS).read_bytes() + b"tampered")
    else:
        (directory / RECORDS).unlink()
    with pytest.raises((SystemExit, FileNotFoundError)):
        verify_backup(directory)
