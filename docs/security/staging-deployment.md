# Protected staging deployment

This is a single-host, isolated staging foundation. It is not a production
deployment and does not provide HA, TLS, SASL, OIDC, encrypted backups, or a
secrets manager yet.

## Secret contract

Provision a private directory outside Git with these files, one value per file:

`POSTGRES_PASSWORD`, `CLICKHOUSE_PASSWORD`, `MINIO_ROOT_PASSWORD`,
`TITAN_READ_TOKEN`, `TITAN_WRITE_TOKEN`, and `TITAN_ADMIN_TOKEN`.

On POSIX hosts:

```bash
mkdir -p .staging-secrets
chmod 700 .staging-secrets
chmod 600 .staging-secrets/*
```

Generate values with a CSPRNG. Never use `titan_dev_password`,
`titan_ci_password`, or `local-demo-token`. On Windows, the preflight refuses
to proceed unless the operator explicitly confirms ACL verification with
`--windows-acl-verified`; mode bits are not treated as Windows ACL enforcement.

The current PostgreSQL, ClickHouse, MinIO, Spark, and Dagster images do not all
support a common Docker `_FILE` convention. The overlay therefore uses
service-compatible Compose interpolation after preflight; the values can still
appear in effective rendered configuration or process environments. This is a
documented limitation, not a claim of secret isolation. A later stage must add
file-aware adapters or an approved injector.

## Preflight and startup

Run the validator before starting any stateful service:

```bash
uv run python scripts/staging_preflight.py \
  --secrets-dir .staging-secrets \
  --compose-file docker-compose.yml \
  --compose-file docker-compose.staging.yml
```

The validator checks required secret files, placeholders, POSIX permissions,
the protected project name, effective merged Compose ports, host bind mounts,
Docker socket mounts, and isolated volume names. It prints no secret values.

The operator must supply the same values to Compose through an approved external
environment/secret injector. The base local stack is never used as the staging
project and no development volume is referenced.

## Isolation

The overlay uses project `titan-staging`, explicit `titan-staging-*` named
volumes, private service networking, and clears host ports for data, API,
dashboard, and observability services. The overlay must not be combined with
`docker-compose.dagster-local.yml`; that override mounts the Docker socket and
is workstation-only.

## Compatibility and limitations

This stage preserves internal plaintext service protocols so existing clients
remain compatible. It does not provide shared access approval. The dashboard
local-only proxy is disabled in the overlay, but shared user authentication is
not implemented yet; do not publish the dashboard or API outside the private
staging network. Kafka, database, MinIO, and ClickHouse authentication remains
credential-based without transport encryption.

Spark checkpoints and recovery artifacts use staging-only resources. Do not
reuse development checkpoints, topics, buckets, databases, or volumes.

## Disposal and rollback

Stop only the staging project. Before any removal, verify the project name and
volume prefix. Roll back by stopping the staging project and restoring the
previous approved environment files. Do not use `down --volumes` against the
development project. A future staging runbook must add encrypted backup and
restore-key procedures before shared data is introduced.

## Required before shared access

Upgrade 6C must add and test TLS/SASL or equivalent service encryption and
identities. Upgrade 6D must add identity-aware dashboard/API access and audit
logging. Upgrade 6E must remove the Dagster socket path. Upgrade 6F must add
authenticated backup encryption and coordinated recovery drills.
