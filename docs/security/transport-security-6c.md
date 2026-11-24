# TITAN Transport Security Foundation — Upgrade 6C-1

**Status:** certificate tooling and compatibility foundation only. TLS is not
enabled in the local or protected staging Compose listeners by this change.

## Current connection inventory

| Client | Destination | Current endpoint | Client/library | Current protection |
|---|---|---|---|---|
| FastAPI/ingestion | Kafka | `kafka:19092` | `confluent-kafka` | plaintext Kafka plus application credentials/profile |
| FastAPI/migrations | PostgreSQL | `postgres:5432` | `psycopg`/SQLAlchemy | plaintext private network plus password |
| FastAPI/warehouse | ClickHouse | `clickhouse:8123` | `clickhouse-connect` | plaintext HTTP plus password |
| Spark/warehouse | MinIO | `minio:9000` | `minio`/S3A | plaintext HTTP plus access key |
| Spark | Kafka/PostgreSQL/MinIO | Compose DNS | Spark Kafka, JDBC/S3A clients | plaintext internal listeners |
| dbt | ClickHouse | `clickhouse:8123` | dbt-clickhouse | plaintext internal listener |
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
