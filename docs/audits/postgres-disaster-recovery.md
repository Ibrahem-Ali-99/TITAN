# PostgreSQL disaster recovery (Phase 4A)

TITAN now has a portable logical-backup workflow for PostgreSQL only. Kafka,
MinIO, ClickHouse, and point-in-time recovery are intentionally out of scope.

## Procedure

```powershell
$env:DATABASE_URL = 'postgresql://user:password@host:5432/titan'
uv run python scripts/postgres_dr.py backup --dsn $env:DATABASE_URL --database titan --output .tmp-backup\2026-11-23
uv run python scripts/postgres_dr.py verify --backup .tmp-backup\2026-11-23
uv run python scripts/postgres_dr.py restore --backup .tmp-backup\2026-11-23 --dsn 'postgresql://user:password@127.0.0.1:55432/titan_restore'
```

Backups use `pg_dump -Fc --no-owner --no-acl`, include a SHA-256 manifest, and
never include credentials. Restore refuses a destination containing tables
unless `--allow-nonempty` is explicitly supplied. The verification output
includes migration version, row counts, a stable event-ID digest, dispositions,
duplicate Kafka-position checks, checkpoint metadata presence, and currency
aggregates. Restore is always demonstrated against a separate disposable
database/volume.

## Evidence

The CI workflow uses PostgreSQL 18 on a disposable service, applies the real
Alembic migrations, inserts synthetic EUR/GBP/USD data plus duplicate and
quarantine records, verifies the backup checksum and `pg_restore --list`, then
restores into a separate database. It also checks corruption detection and
non-empty destination refusal.

Logical snapshots provide a snapshot-level RPO only. They do not provide WAL
archiving or point-in-time recovery. RPO is the interval between successful
backups; RTO includes provisioning PostgreSQL, transfer, restore, migrations
and verification. Backup size, duration and restore duration must be recorded
from the machine-readable evidence rather than estimated.

Backup files should be stored outside Git with encryption and access control
provided by the operator's approved storage system. On POSIX systems the
scripts apply mode `0600`; Windows relies on the destination ACL.
