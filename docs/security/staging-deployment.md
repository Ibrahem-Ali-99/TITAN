# Protected staging deployment

This is a single-host, isolated staging foundation. It is not a production
deployment and does not provide HA, OIDC, encrypted backups, or a secrets
manager. PostgreSQL TLS and Kafka SASL/TLS are opt-in overlays; the default
staging stack remains compatible with local plaintext clients.

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

The default protected staging overlay preserves internal plaintext service
protocols so existing clients
remain compatible. It does not provide shared access approval. The dashboard
local-only proxy is disabled in the overlay, but shared user authentication is
not implemented yet; do not publish the dashboard or API outside the private
staging network. Kafka, database, MinIO, and ClickHouse authentication remains
credential-based without transport encryption.

The opt-in `docker-compose.staging-postgres-tls.yml` overlay is the verified
PostgreSQL transport exception. It requires a separately seeded
`titan-staging-postgres-tls` volume containing `ca.crt`, `postgres.crt`,
`postgres.key`, and TLS-only `pg_hba.conf`. Clients use `sslmode=verify-full`
with `/tls/ca.crt`; PostgreSQL private-key ownership must match the pinned
image's `postgres` UID. Use the TLS-specific preflight flags before startup.
This overlay does not encrypt Kafka, ClickHouse, MinIO, observability, or
recovery traffic.

The opt-in `docker-compose.staging-kafka-tls.yml` overlay is a separate
protected Kafka transport option. It requires the generated
`titan-staging-kafka-tls` named volume and `KAFKA_SASL_USERNAME`,
`KAFKA_SASL_PASSWORD`, and `KAFKA_TLS_STORE_PASSWORD`. The broker advertises
only `SASL_SSL://kafka:19092`; configured API, ingestion, monitoring and Spark
clients use certificate verification and SASL/PLAIN. No host Kafka port is
published. Generate JKS stores with `scripts/staging_kafka_tls.py`, validate
the effective Compose configuration with `scripts/staging_preflight.py
--kafka-tls`, and never reuse the development Kafka volume. This overlay does
not encrypt PostgreSQL unless the PostgreSQL TLS overlay is also selected, nor
does it protect ClickHouse, MinIO, observability or host-side recovery traffic.

Spark checkpoints and recovery artifacts use staging-only resources. Do not
reuse development checkpoints, topics, buckets, databases, or volumes.

## Disposal and rollback

Stop only the staging project. Before any removal, verify the project name and
volume prefix. Roll back by stopping the staging project and restoring the
previous approved environment files. Do not use `down --volumes` against the
development project. A future staging runbook must add encrypted backup and
restore-key procedures before shared data is introduced.

## Required before shared access

Kafka TLS material must be seeded before Compose startup:

```bash
uv run python scripts/staging_kafka_tls.py --source .staging-kafka-tls \
  --volume titan-staging-kafka-tls --password '<store-password>' \
  --username titan-staging --sasl-password '<generated-sasl-password>'
```

The host-side helper uses the pinned Kafka image, rejects nonempty or partial
volumes, sets UID 1000 ownership and mode 0600, and verifies all required JKS,
credential, JAAS, client-property, and CA files.

Upgrade 6C must add and test TLS/SASL or equivalent service encryption and
identities. Upgrade 6D must add identity-aware dashboard/API access and audit
logging. Upgrade 6E must remove the Dagster socket path. Upgrade 6F must add
authenticated backup encryption and coordinated recovery drills.
# ClickHouse TLS staging overlay

Protected staging uses `docker-compose.staging-clickhouse-tls.yml`. Generate the
normal disposable staging certificates, copy the example XML to the generated
directory as `config.xml`, then seed the empty named volume with:

```powershell
python scripts/staging_clickhouse_tls.py --source .tmp-staging-tls --volume titan-staging-clickhouse-tls
```

The command validates the CA chain, `clickhouse` SAN, expiry and key match before
copying `ca.crt`, `clickhouse.crt`, `clickhouse.key`, and `config.xml`. It rejects
nonempty volumes. The private key is owned by ClickHouse UID/GID `101:101` and
mode `0600`; the CA, server certificate, and non-secret XML configuration are
`0644` so client containers can read the trust material. Never make the private
key world-readable.

The ClickHouse server mounts the TLS volume read-only at `/tls` and read-write at
the image’s `config.d` include directory because the image writes
`config-preprocessed.xml` there at startup; the volume is still disposable and
seeded only after validation. All application clients mount it read-only. The
overlay disables the plaintext HTTP and native listeners and exposes HTTPS on
the internal-only `clickhouse:8443` endpoint. Clients use `CLICKHOUSE_SECURE=true`,
`CLICKHOUSE_VERIFY=true`, `CLICKHOUSE_CA_CERT=/tls/ca.crt`, and
`CLICKHOUSE_SERVER_HOST_NAME=clickhouse`. dbt-clickhouse 1.10.3 does not expose a
CA-path profile field, so its protected staging process must set
`SSL_CERT_FILE=/tls/ca.crt`; it still uses `secure: true`, `verify: true`, and
the ClickHouse hostname. Local development is unchanged.
