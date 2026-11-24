# TITAN Transport Security Foundation — Upgrade 6C-1

**Status:** certificate tooling and compatibility foundation only. TLS is not
enabled in the local or protected staging Compose listeners by this change.

## Current connection inventory

| Client | Destination | Current endpoint | Client/library | Current protection |
|---|---|---|---|---|
| FastAPI/ingestion | Kafka | `kafka:19092` | `confluent-kafka` | plaintext Kafka plus application credentials/profile |
| FastAPI/migrations | PostgreSQL | `postgres:5432` | `psycopg`/SQLAlchemy | plaintext private network plus password |
| FastAPI/warehouse | ClickHouse | `clickhouse:8443` in protected staging | `clickhouse-connect` | HTTPS with CA and hostname verification; local profile remains HTTP |
| Spark/warehouse | MinIO | `minio:9000` | `minio`/S3A | plaintext HTTP plus access key |
| Spark | Kafka/PostgreSQL/MinIO | Compose DNS | Spark Kafka, JDBC/S3A clients | plaintext internal listeners |
| dbt | ClickHouse | `clickhouse:8443` in protected staging | dbt-clickhouse | secure/verify plus `SSL_CERT_FILE`; local profile remains plaintext |
| Dagster | PostgreSQL/ClickHouse/MinIO | Compose DNS | psycopg/clickhouse/minio | plaintext internal listeners |
| Prometheus/Grafana/Tempo | API/exporters/OTel | Compose DNS | HTTP clients | private network; no TLS |
| Host operator/tools | published development ports only | loopback | CLI/Python | workstation boundary |

The staging overlay removes host publication for infrastructure services. The
current clients support certificate verification through their native options,
but the exact Kafka SASL/TLS, PostgreSQL SSL mode, ClickHouse TLS, MinIO HTTPS,
Spark S3A, and dbt settings must be enabled in later migration steps.

## Certificate model

`scripts/staging_tls.py` provisions a disposable RSA CA and server certificates
for Kafka, PostgreSQL, ClickHouse, and MinIO. Each certificate contains SANs for
the Compose DNS name and its `*.titan-staging` alias. The tool uses the system
OpenSSL executable, never custom cryptography, and refuses to overwrite a
non-empty directory.

Private keys are generated outside Git. On POSIX, keys are restricted to owner
read/write. On Windows, mode bits are not treated as ACL enforcement: callers
must explicitly attest that ACLs were verified with `--windows-acl-verified`.
Use an external protected directory or secret injector for shared staging.

Provision and verify disposable material:

```powershell
.venv\Scripts\python.exe scripts\staging_tls.py provision --tls-dir .staging-tls --days 30 --windows-acl-verified
.venv\Scripts\python.exe scripts\staging_tls.py verify --tls-dir .staging-tls --windows-acl-verified
```

When a future TLS-required overlay is introduced, preflight must include:

```powershell
.venv\Scripts\python.exe scripts\staging_preflight.py --require-tls --tls-dir .staging-tls ...
```

That check verifies presence; the certificate tool performs chain, SAN, expiry,
and key-pair validation. Missing or invalid material must fail before protected
services start. No client may use `verify=False`, `CERT_NONE`, or disabled host
name checking.

## Lifecycle

Renew before the configured expiry threshold by provisioning a new version in a
new directory, validating every SAN and client trust path, switching consumers
in a bounded maintenance window, and deleting the old private key only after
rollback is no longer required. Revoke through the CA or external PKI process;
this tool does not invent a revocation service or claim CRL/OCSP coverage.

The CA private key requires separate operator custody. Certificates do not imply
client authentication; Kafka SASL, database roles, ClickHouse users, MinIO
policies, API scopes, and future mTLS identities remain separate controls.

## Rollout order and remaining plaintext

1. Import/provision certificates and distribute CA trust bundles.
2. Enable and test server-side TLS for PostgreSQL, ClickHouse, and MinIO.
3. Enable Kafka TLS/SASL with dual listener/configuration migration.
4. Configure Spark, dbt, Dagster, API, ingestion, and recovery clients with
   verified trust paths.
5. Add negative tests for expiry, hostname, chain, key mismatch, and bad
   credentials before switching protected staging.
6. Add proxy HTTPS and external user authentication in the later identity stage.

Until those stages are complete, all internal data services, observability
traffic, and recovery tooling remain plaintext inside the isolated staging
network. TLS is not a substitute for authentication, authorization, secret
management, or network isolation.

## Kafka SASL/TLS opt-in overlay

Upgrade 6C-3 adds `docker-compose.staging-kafka-tls.yml`. It replaces the
staging broker's plaintext container listener with `SASL_SSL://kafka:19092`
and keeps the controller listener private. The selected mechanism is
SASL/PLAIN over TLS: Kafka 4.x supports it across the Apache broker, the
Confluent Python client, and the Spark Kafka connector without a post-start
KRaft user-bootstrap race. It is not a claim that PLAIN is safe without TLS.

The overlay requires disposable credentials, a truststore password, and a
named volume `titan-staging-kafka-tls` containing the JKS keystore/truststore
and `ca.crt`. `scripts/staging_kafka_tls.py` converts verified PEM material to
those stores and refuses to overwrite a nonempty destination. The broker
healthcheck uses an authenticated client properties file; API, ingestion,
monitoring and Spark receive explicit SASL/TLS options. The shared client
configuration rejects unsupported protocols, missing credentials, missing CA
paths, and mechanisms other than the selected PLAIN mechanism. Spark uses the
JKS truststore and hostname verification.

Provisioning is separate from Compose so private material is not committed:

```bash
uv run python scripts/staging_kafka_tls.py --source .staging-tls \
  --destination .staging-kafka-tls --password '<generated-store-password>' \
  --username titan-staging --sasl-password '<generated-sasl-password>'
```

Copy the generated directory into the disposable named volume with a trusted
operator-side container command, then run preflight with `--kafka-tls` and the
three Compose files. Wrong credentials, wrong CA, hostname mismatch, missing
stores, or a plaintext bootstrap must fail before protected startup.

SCRAM remains a future option if broker initialization is changed to seed users
before the KRaft log starts. Replay and backup tools must receive the same
explicit SASL/TLS environment; no tool may silently fall back when
`KAFKA_SECURITY_PROTOCOL=SASL_SSL`.

## MinIO HTTPS opt-in overlay

Upgrade 6C-5 adds `docker-compose.staging-minio-tls.yml`. It removes MinIO
host publication and mounts a disposable `titan-staging-minio-tls` volume at
the image's certificate directory. `scripts/staging_minio_tls.py` validates
the CA chain, the `minio` DNS SAN, expiry, and certificate/key matching before
seeding an empty volume with `public.crt`, `private.key`, and `CAs/ca.crt`.
The private key is mode 0600; it is never committed or printed.

Protected Python clients use `https://minio:9000`, `SSL_CERT_FILE=/tls/ca.crt`,
and the SDK's default certificate verification. The warehouse S3 filesystem
receives the same CA explicitly. Spark S3A enables SSL and uses a JKS
truststore mounted separately from the Kafka TLS volume. Wrong CA, hostname
mismatch, invalid credentials, and plaintext access must fail closed. Local
development remains HTTP and is not changed by this overlay.

## PostgreSQL TLS opt-in overlay

Upgrade 6C-2 adds `docker-compose.staging-postgres-tls.yml`. It is not part of
the default staging command. Before starting it, provision and verify the
certificate foundation, then copy only `ca.crt`, `postgres.crt`, and
`postgres.key`, and a TLS-only `pg_hba.conf` into the isolated named volume
`titan-staging-postgres-tls`.
The PostgreSQL container reads the key as its actual database user; the key is
never made world-readable. Client containers mount the CA read-only and use
`sslmode=verify-full` with `sslrootcert=/tls/ca.crt`. The SAN must include
`postgres`, because that is the Compose DNS name used by clients.

The protected preflight command must include all three compose files and
`--postgres-tls`; it rejects a server without `ssl=on` or any client that lacks
`verify-full` and the explicit CA path. The overlay does not publish port 5432.

Rotation is performed by seeding a new isolated TLS volume, validating the new
certificate and client connections, then recreating only the protected staging
project. Roll back by restoring the previous volume before expiry. Development
PostgreSQL remains unchanged. The PostgreSQL TLS overlay does not encrypt Kafka,
ClickHouse, MinIO, observability, or host-side recovery traffic; those remain
future phases.
