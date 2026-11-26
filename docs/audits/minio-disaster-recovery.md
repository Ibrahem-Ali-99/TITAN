# MinIO data lake disaster recovery (Phase 4B)

TITAN's MinIO backup covers object bytes and metadata for the Bronze and Silver
lake prefixes. The reusable commands are:

```powershell
$env:MINIO_ENDPOINT = '127.0.0.1:9000'
$env:MINIO_ACCESS_KEY = 'titan'
$env:MINIO_SECRET_KEY = 'local-only-secret'
uv run python -m scripts.minio_dr backup --bucket titan-lake --output .tmp-minio-backup\run-1
uv run python -m scripts.minio_dr verify --backup .tmp-minio-backup\run-1
uv run python -m scripts.minio_dr restore --backup .tmp-minio-backup\run-1 --destination-bucket titan-lake-restore
```

The backup stores exact object bytes under `objects/` and a manifest containing
object keys, sizes, SHA-256 hashes and user metadata. S3 ETags are not used as
checksums. Restore verifies every uploaded object and writes a completion marker
only after all objects and hashes succeed. A failed restore removes objects it
created. Destination buckets must end in `-restore` or `_restore` and existing
nonempty buckets are rejected unless explicitly overridden.

## Recovery classification

| Class | Objects | Recovery treatment |
| --- | --- | --- |
| Authoritative | committed Bronze batch prefixes and Silver version prefixes | Back up and checksum-verify. Bronze visibility still depends on PostgreSQL `bronze_batch_commits`; Silver authority is PostgreSQL `lake_publications`. |
| Reproducible/convenience | `silver/current/` | Backed up for operational convenience, but PostgreSQL's published publication row wins if it disagrees. |
| Temporary | staging files, incomplete prefixes and Spark local checkpoints | Not treated as authoritative. Spark checkpoints are on the `spark_checkpoints` Docker volume, not MinIO, and require coordinated Kafka/PostgreSQL recovery. |

The CI workflow uses isolated MinIO buckets and deterministic Bronze/Silver
Parquet fixtures. It verifies object counts, byte hashes, Parquet schemas,
row counts, event identities, lineage keys, completion-marker behavior,
corruption rejection, incomplete-backup rejection, and nonempty-destination
refusal. It never touches the development bucket.

Hosted CI pins `bitnamilegacy/minio` by immutable digest because the former
development image reference was not pullable by GitHub-hosted runners. The
replacement was started locally with the same credentials and health probe;
the image reports MinIO `DEVELOPMENT.2024-10-29T16-01-48Z`. This is a CI-only
compatibility choice, not a production image recommendation.

This is not complete platform disaster recovery. Kafka offsets, PostgreSQL
dispositions and checkpoints must be recovered as a coordinated set. A lake
restore alone must not restart Spark against an incompatible checkpoint or
advertise Silver as current. Logical/object snapshots provide an RPO equal to
backup frequency; they do not provide MinIO version-history retention or
point-in-time recovery. RTO includes object transfer and Parquet verification.
## Confidentiality controls

MinIO backup trees and Parquet payloads are restricted to `0700`/`0600` on POSIX
systems, with CI using `umask 077`. Windows requires an ACL-restricted backup
root. Object checksums detect corruption but do not encrypt the copied bytes.
