# Local infrastructure

Docker Desktop with the WSL2 backend is the expected runtime. Give Docker at least **6 GB** of memory and **2 CPUs**. **8 GB** is the comfortable setting for this Compose file. Kafka is capped near 768 MB with a 512 MB heap, ClickHouse near 1.5 GB, and Postgres near 512 MB. The rest is the API, the consumer, MinIO, and the Next.js dev server.

This stack does not include Spark, orchestration, or a warehouse load. ClickHouse starts with an empty `titan` database and no tables. MinIO starts with no buckets. Their health checks only prove the process and, for the verification script, the configured credentials.

## Ports

| Service | Host port | Container address | Published? |
| --- | --- | --- | --- |
| Kafka clients on the host | 9092 | `localhost:9092` (`PLAINTEXT_HOST`) | yes |
| Kafka clients in Compose | — | `kafka:19092` (`PLAINTEXT`) | no |
| Kafka controller | — | `kafka:29093` | no |
| PostgreSQL | 5435 | `postgres:5432` | yes |
| ClickHouse HTTP | 8123 | `clickhouse:8123` | yes |
| MinIO API | 9000 | `minio:9000` | yes |
| MinIO console | 9001 | `minio:9001` | yes |
| FastAPI | 8000 | `api:8000` | yes |
| Next.js | 3000 | `web:3000` | yes |

Host tools must use `localhost:9092`. Containers must use `kafka:19092`. A container that uses `localhost` reaches itself, not Kafka.

PostgreSQL is published on host port **5435** (`POSTGRES_HOST_PORT`). Containers keep using `postgres:5432`. The Day 1 workstation already had listeners on 5432, 5433, and 5434, and a published 5432 was answered by a local PostgreSQL with a different password. Host `DATABASE_URL` must use the same port as `POSTGRES_HOST_PORT`.

PostgreSQL 18 stores data under `/var/lib/postgresql`. The named volume is mounted there.

## MinIO image

The official `minio/minio` repository is no longer anonymously pullable from Docker Hub, and the pinned Quay tag used by older guides was not available from this machine. Compose uses the community image `alpine/minio:RELEASE.2025-10-15T17-29-55Z`, which does pull. Treat that as a local-dev supply-chain compromise, not as a production base image.

That image runs as the `minio` user. A fresh Docker named volume is owned by root, so the process exits with `file access denied` while creating `/data/.minio.sys`. Compose sets `user: "0:0"` so the local process can initialize the volume. This is not privileged mode and it is not a production hardening choice.

## Reset

`docker compose down -v` deletes Kafka, Postgres, ClickHouse, and MinIO volumes. The Kafka cluster id is fixed so a restart without `-v` reuses the formatted log directory.
