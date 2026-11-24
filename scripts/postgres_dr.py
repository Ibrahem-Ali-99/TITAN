"""Portable PostgreSQL logical backup, restore, and integrity verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from dr_security import secure_directory, secure_file


def _run(command: list[str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=capture)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _pg_version(dsn: str) -> tuple[str, str]:
    with psycopg.connect(dsn) as connection:
        row = connection.execute("SELECT version(), current_setting('server_version')").fetchone()
    assert row is not None
    return str(row[0]), str(row[1])


def _migration_version(connection: psycopg.Connection[Any]) -> str | None:
    row = connection.execute("SELECT version_num FROM alembic_version LIMIT 1").fetchone()
    return None if row is None else str(row[0])


def _scalar(row: tuple[Any, ...] | None) -> Any:
    if row is None:
        raise RuntimeError("expected a database row")
    return row[0]


def _manifest_path(directory: Path) -> Path:
    return directory / "manifest.json"


def _assert_safe_restore_destination(dsn: str) -> None:
    info = conninfo_to_dict(dsn)
    host = str(info.get("host", "localhost")).lower()
    database = str(info.get("dbname", "")).lower()
    if host not in {"localhost", "127.0.0.1", "::1"}:
        raise SystemExit("restore destination must be a local disposable PostgreSQL host")
    if not (database.endswith("_restore") or database.endswith("_dr")):
        raise SystemExit("restore destination database must end in _restore or _dr")


def backup(args: argparse.Namespace) -> None:
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    secure_directory(output)
    started = time.perf_counter()
    server_version_text, server_version = _pg_version(args.dsn)
    archive = output / "titan-postgres.dump"
    _run(
        [
            "pg_dump",
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--dbname",
            args.dsn,
            "--file",
            str(archive),
        ]
    )
    duration_ms = round((time.perf_counter() - started) * 1000)
    manifest = {
        "backup_id": output.name,
        "created_at": datetime.now(UTC).isoformat(),
        "database": args.database,
        "format": "pg_dump custom (-Fc)",
        "archive": archive.name,
        "archive_bytes": archive.stat().st_size,
        "sha256": _sha256(archive),
        "server_version": server_version,
        "server_version_text": server_version_text,
        "migration_version": _migration_version_from_dsn(args.dsn),
        "backup_duration_ms": duration_ms,
        "tool": "scripts/postgres_dr.py",
    }
    _manifest_path(output).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    secure_file(archive)
    secure_file(_manifest_path(output))
    print(json.dumps(manifest, indent=2))


def verify_backup(directory: Path) -> dict[str, Any]:
    manifest = json.loads(_manifest_path(directory).read_text(encoding="utf-8"))
    archive = directory / str(manifest["archive"])
    if not archive.is_file():
        raise SystemExit(f"backup archive missing: {archive}")
    actual = _sha256(archive)
    if actual != manifest["sha256"]:
        raise SystemExit("backup checksum mismatch")
    listing = _run(["pg_restore", "--list", str(archive)]).stdout
    if "TABLE DATA" not in listing and "TABLE" not in listing:
        raise SystemExit("backup archive contains no table entries")
    return {**manifest, "verified": True, "restore_list_entries": len(listing.splitlines())}


def _migration_version_from_dsn(dsn: str) -> str | None:
    with psycopg.connect(dsn) as connection:
        return _migration_version(connection)


def verify(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    result = verify_backup(args.backup.resolve())
    result["verification_ms"] = round((time.perf_counter() - started) * 1000)
    print(json.dumps(result, indent=2))


def integrity(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as connection:
        tables = [
            "commerce_events",
            "ingestion_quarantine",
            "ingestion_observations",
            "spark_job_status",
            "lake_publications",
            "bronze_batch_commits",
            "pipeline_runs",
            "warehouse_loads",
        ]
        counts: dict[str, int] = {}
        for table in tables:
            counts[table] = int(
                _scalar(
                    connection.execute(
                        """SELECT count(*) FROM information_schema.tables
                           WHERE table_schema='public' AND table_name=%s""",
                        (table,),
                    ).fetchone()
                )
            )
        row_counts = {
            table: _scalar(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone())
            for table, present in counts.items()
            if present
        }
        event_ids = [
            str(row[0])
            for row in connection.execute("SELECT event_id FROM commerce_events ORDER BY event_id")
        ]
        dispositions: dict[str, int] = dict(
            connection.execute(
                "SELECT disposition, count(*) FROM commerce_events GROUP BY disposition"
            )
        )
        revenue: dict[str, Any] = dict(
            connection.execute(
                """SELECT COALESCE(event_envelope->'payload'->>'currency', 'UNKNOWN'),
                          COALESCE(sum((event_envelope->'payload'->>'total_amount')::numeric), 0)
                   FROM commerce_events WHERE disposition='accepted'
                   GROUP BY 1 ORDER BY 1"""
            )
        )
        duplicate_positions = int(
            _scalar(
                connection.execute(
                    """SELECT count(*) FROM (SELECT kafka_topic, kafka_partition, kafka_offset
                       FROM commerce_events GROUP BY 1,2,3 HAVING count(*) > 1) duplicates"""
                ).fetchone()
            )
        )
        fk_violations = int(
            _scalar(
                connection.execute(
                    """SELECT count(*) FROM pg_constraint
                       WHERE contype='f' AND connamespace='public'::regnamespace"""
                ).fetchone()
            )
        )
        return {
            "migration_version": _migration_version(connection),
            "row_counts": row_counts,
            "event_count": len(event_ids),
            "event_ids_sha256": hashlib.sha256("\n".join(event_ids).encode()).hexdigest(),
            "dispositions": dispositions,
            "currency_revenue": {str(k): str(v) for k, v in revenue.items()},
            "duplicate_kafka_positions": duplicate_positions,
            "foreign_key_constraint_count": fk_violations,
        }


def restore(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    _assert_safe_restore_destination(args.dsn)
    backup_dir = args.backup.resolve()
    evidence = verify_backup(backup_dir)
    with psycopg.connect(args.dsn) as connection:
        nonempty = int(
            _scalar(
                connection.execute(
                    """SELECT count(*) FROM pg_class
                       WHERE relkind IN ('r','p') AND relnamespace='public'::regnamespace"""
                ).fetchone()
            )
        )
    if nonempty and not args.allow_nonempty:
        raise SystemExit(
            "refusing to restore into a nonempty destination; use --allow-nonempty explicitly"
        )
    archive = backup_dir / str(evidence["archive"])
    _run(
        [
            "pg_restore",
            "--exit-on-error",
            "--no-owner",
            "--no-acl",
            "--dbname",
            args.dsn,
            str(archive),
        ]
    )
    result = {
        "backup_id": evidence["backup_id"],
        "restore_duration_ms": round((time.perf_counter() - started) * 1000),
        "integrity": integrity(args.dsn),
    }
    print(json.dumps(result, indent=2))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    sub = root.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup")
    b.add_argument("--dsn", required=True)
    b.add_argument("--database", required=True)
    b.add_argument("--output", type=Path, required=True)
    b.set_defaults(func=backup)
    v = sub.add_parser("verify")
    v.add_argument("--backup", type=Path, required=True)
    v.set_defaults(func=verify)
    r = sub.add_parser("restore")
    r.add_argument("--backup", type=Path, required=True)
    r.add_argument("--dsn", required=True)
    r.add_argument("--allow-nonempty", action="store_true")
    r.set_defaults(func=restore)
    return root


if __name__ == "__main__":
    try:
        parsed = parser().parse_args()
        parsed.func(parsed)
    except KeyboardInterrupt:
        sys.exit(130)
