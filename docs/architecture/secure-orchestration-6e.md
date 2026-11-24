# Secure orchestration and protected-staging readiness

## Image and Compose contract

The protected stack is rendered from `docker-compose.yml` followed by
`docker-compose.staging.yml` and the required TLS/ingress overlays. The base
file is mandatory: it supplies the pinned infrastructure images and the
`titan-orchestration` image/build contract. The orchestration image is built from
`infrastructure/docker/orchestration.Dockerfile`, installs the frozen workspace
with `uv sync --frozen`, runs as UID/GID 1000, and protected staging requires
`TITAN_ORCHESTRATION_IMAGE` to resolve to an immutable digest.

The previous `service "dagster-daemon" has neither an image nor a build context`
error came from evaluating an overlay without the base file. A complete merge
with the base file succeeds; the startup procedure now documents this ordering.

## Execution boundary

The protected Dagster daemon is intentionally socket-free. It can load the
definitions and record schedules/run metadata, but it is not a Docker control
plane. Spark Bronze and Silver are launched by a trusted host-side Dagster
process using the same disposable Compose project and the explicitly selected
protected overlays. This preserves the existing asset graph while keeping the
Docker daemon outside the Dagster container boundary.

The host-side launcher is an operator-controlled trust boundary. It accepts only
the checked-in Bronze and Silver Compose services, the protected project name,
and the checked-in Compose file set. It must not be exposed as a network API and
must not accept arbitrary images, commands, mounts, privileges, or project
names. The local socket override remains a separate development-only option and
is not part of protected staging.

## Acceptance contract

Before a run, the operator must:

1. Render the complete Compose file set with the base file first.
2. Run `scripts/staging_preflight.py` with disposable secrets and all required
   TLS material.
3. Confirm the Dagster service has no `/var/run/docker.sock` mount and no host
   ports are published for infrastructure services.
4. Launch the host-side Dagster process with the protected Compose environment.
5. Materialize Bronze, Silver, and the existing warehouse asset graph, then
   verify run status, publication metadata, counts, and cleanup.

An unavailable host launcher is a bounded, observable failure; it is not a
reason to re-enable a socket in the protected daemon.
