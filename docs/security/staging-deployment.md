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
project and no development volume is referenced. Protected staging also requires
`TITAN_ORCHESTRATION_IMAGE` to be an immutable image reference, normally a
verified registry digest; the local build-only default is not accepted.

Protected staging Compose files are overlays and must always be evaluated with
`docker-compose.yml` first. The base file supplies the pinned service images and
the reproducible `titan-orchestration` build contract; invoking an overlay alone
can produce misleading errors such as a missing Dagster image or build context.
The protected Dagster container has no Docker socket. Real Spark materialization
is run by the trusted host-side Dagster launcher against the disposable staging
Compose project, while the containerized daemon is limited to definitions,
schedules, and run metadata.

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
uv run python -m scripts.staging_kafka_tls --source .staging-kafka-tls \
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
python -m scripts.staging_clickhouse_tls --source .tmp-staging-tls --volume titan-staging-clickhouse-tls
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

## MinIO HTTPS staging

Generate fresh staging certificates with `scripts/staging_tls.py`, then seed a
new empty volume without manually copying files:

```powershell
python -m scripts.staging_minio_tls --source .tmp-staging-tls --volume titan-staging-minio-tls
```

The helper rejects missing, expired, untrusted, mismatched, partial, or
nonempty material. It uses the pinned MinIO image for provisioning, sets the
private key to mode 0600, and stores the public certificate and CA as mode
0644. Protected Compose uses `https://minio:9000` with no host MinIO ports;
Python clients receive `/tls/ca.crt`, while Spark S3A receives a separate
truststore. Run preflight with `--minio-tls --minio-tls-dir` before startup.

Renewal uses a new disposable volume and a fresh certificate. Validate a
complete client round-trip before replacing the protected staging project;
rollback is the same operation against the previous approved volume. Existing
development MinIO remains HTTP and its volume is not changed.

## PostgreSQL TLS volume provisioning

Provision the PostgreSQL server certificate into a fresh named volume before
starting the protected project:

```powershell
python -m scripts.staging_postgres_tls --source .tmp-staging-tls --volume titan-staging-postgres-tls
```

The command validates the `postgres` SAN, CA chain, expiry, and key match,
creates a TLS-only `pg_hba.conf`, rejects nonempty volumes, and applies the
PostgreSQL image's runtime ownership with mode 0600 for the private key. Run
preflight with `--postgres-tls --postgres-tls-dir` and include the PostgreSQL
overlay before startup. Development PostgreSQL volumes are never used.

## Current-source Spark image

Protected staging removes the source bind mounts from the Spark services. Build
the application image from the current checkout before startup:

```powershell
$rev = git rev-parse HEAD
docker build --build-arg TITAN_SOURCE_REVISION=$rev `
  --file infrastructure/docker/spark.Dockerfile `
  --tag titan-spark:protected-$rev .
```

Set `TITAN_SPARK_IMAGE=titan-spark:protected-$rev` for the protected Compose
run. The image label `org.opencontainers.image.revision` must equal the
checkout revision; do not use an older local image or add application source
bind mounts to protected staging.

## Protected HTTPS ingress

The opt-in `docker-compose.staging-ingress.yml` overlay adds the only
browser-facing endpoint. It uses the pinned nginx image, terminates TLS with
the generated `ingress` certificate, and publishes only
`127.0.0.1:8443:8443`. API, database, Kafka, MinIO, ClickHouse and
observability services remain private on the Compose network; FastAPI is not
published directly.

Generate fresh material and seed the dedicated ingress volume before startup:

```powershell
python -m scripts.staging_tls provision --tls-dir .tmp-staging-tls --days 2 --windows-acl-verified
python -m scripts.staging_ingress_tls --source .tmp-staging-tls --volume titan-staging-ingress-tls
python -m scripts.staging_preflight --secrets-dir .staging-secrets --project-name titan-staging `
  --ingress-tls --ingress-tls-dir .tmp-staging-tls `
  --compose-file docker-compose.yml `
  --compose-file docker-compose.staging.yml `
  --compose-file docker-compose.staging-ingress.yml
docker compose -f docker-compose.yml -f docker-compose.staging.yml `
  -f docker-compose.staging-ingress.yml up --detach --wait ingress
```

Trust `.tmp-staging-tls/ca.crt` in the disposable test browser or use a
certificate-verifying client with `https://localhost:8443` (or
`https://staging.local:8443` with local name resolution). Missing, expired,
untrusted, or mismatched ingress material causes preflight or nginx startup
to fail. The Next.js server-side proxy continues to strip browser-supplied
authorization and inject the scoped token allowed by the verified session.
The proxy keeps the token server-side and requires the admin role for
administrative calls.

Renewal uses a newly generated CA/certificate and a new disposable volume;
validate the browser and authenticated API round-trip before replacing the
staging project. This is a loopback-only trusted-workstation boundary, not
shared-staging or public HTTPS, and it does not provide OIDC or an external
user identity in local mode. Protected staging now requires the OIDC settings
documented in `docs/security/oidc-rbac-6d.md`; do not start it with anonymous
or placeholder identity configuration.

## Disposable protected browser acceptance

Run from the repository root with Docker Desktop running Linux containers.
The harness provisions a fresh Compose project, test identity provider and CA.
The first command covers navigation and logout. The second checks the four
role boundaries. Each invocation uses its own disposable project. Let the
harness generate the project name.

```powershell
uv run python -m scripts.accept_protected_browser_runtime --browser-smoke --container-browser
uv run python -m scripts.accept_protected_browser_runtime --browser-smoke --container-browser --rbac-smoke
```

Keep the generated evidence under the ignored `artifacts/` directory. The
browser verifies the ingress certificate through the disposable CA.
