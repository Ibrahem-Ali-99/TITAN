# Dagster Docker socket security

## Scope and finding

The Dagster daemon image runs `pipelines/dagster/src/titan_dagster/definitions.py`. Its `_compose()` helper executes `docker compose --profile processing run --rm spark-bronze` and `spark-silver`; this is the only repository code path that requires Docker daemon access from inside Dagster. The warehouse and database assets use PostgreSQL, ClickHouse, and MinIO over their normal service endpoints. GitHub Actions launches Compose from the runner, not from Dagster.

An attacker with code execution in the daemon and the socket mounted could use the Docker API to create privileged containers, mount host paths, read other container environments, join application networks, and control or remove volumes. A read-only socket mount would not be a security boundary because Docker API mutation requests are still available through the socket. This is root-equivalent access to the Docker host.

## Architecture decision

The repository now uses option C from the security review: the protected/default `orchestration` profile has no Docker socket mount. The existing Spark-launching behavior remains available through the explicitly named `docker-compose.dagster-local.yml` override for a trusted workstation operator:

```powershell
docker compose -f docker-compose.yml -f docker-compose.dagster-local.yml --profile orchestration up -d dagster-daemon
```

The override is intentionally not part of any protected deployment, CI workflow, clone overlay, recovery workflow, or production configuration. It is not a complete privilege-removing redesign: when the override is selected, Dagster retains root-equivalent Docker control. The safer long-term option A is to move Spark lifecycle management to a trusted host-side launcher and have Dagster call a narrow authenticated service or submit to an external scheduler; that is outside this scoped change.

## Before and after

| Configuration | Dagster Docker API access | Effect |
| --- | --- | --- |
| Before | Always mounted in the base orchestration service | Any process compromise in the daemon could control the Docker host |
| Default/protected after | No socket | Dagster can use configured data services but cannot create or inspect Docker containers |
| Explicit local override after | Socket mounted | Existing Compose-launched Spark assets continue to work for a trusted local operator |

The socket is not exposed over TCP. The default service still communicates with Kafka, PostgreSQL, MinIO, and ClickHouse over the internal Compose network. No development volumes were reset or migrated.

## Validation

- Static security regression tests assert the base Dagster service has no socket mount and that the mount exists only in the explicit local override.
- Dagster definitions validation must be run with `DAGSTER_HOME` pointing at a disposable directory and the repository source paths in `PYTHONPATH`.
- Both protected and explicit-local Compose configurations must render successfully. The protected rendered configuration must contain no `/var/run/docker.sock` entry for `dagster-daemon`.
- The existing pipeline and recovery workflows remain host-launched and do not inherit the local override.

## Residual risks and limitations

The local override is intentionally dangerous and is not a sandbox. It should be used only on a trusted workstation with no untrusted Dagster code or schedules. Removing the socket entirely requires changing the asset execution architecture; until that work is done, scheduled Spark execution in the containerized daemon requires the explicit override. This change does not make the deployment production-secure, add TLS, or replace local credentials with a secret manager.
