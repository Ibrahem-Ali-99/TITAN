# Supply-chain security audit and Upgrade 5E

## Scope

This review covers repository Dockerfiles and Compose files, GitHub Actions, `pyproject.toml`/`uv.lock`, the frontend lockfile, the Spark image build, connector downloads, and existing secret/vulnerability scanners.

## Changes implemented

| Surface | Previous reference | Updated reference/evidence |
| --- | --- | --- |
| GitHub Actions | `actions/checkout@v7`, `astral-sh/setup-uv@v7`, `actions/setup-node@v7`, `actions/upload-artifact@v7`, Docker actions on `v3` | Full 40-character commit SHAs, with version comments; resolved from the corresponding upstream tags on 2026-11-23 |
| Compose runtime images | Version tags for Kafka, PostgreSQL, ClickHouse, MinIO, Prometheus, Grafana, Tempo, OTel, and exporters | Registry `RepoDigest` values recorded directly from Docker Engine; readable version comments remain beside each digest |
| Python/uv bases | `python:3.12.13-slim-bookworm`, `ghcr.io/astral-sh/uv:0.12.1` | Immutable linux/amd64 manifest digests from `docker buildx imagetools inspect` |
| Web base | `node:26.13.1-bookworm-slim` | Immutable linux/amd64 manifest digest from `docker buildx imagetools inspect` |
| Spark base and connectors | Already digest-pinned Spark base; Maven downloads had SHA-1 checks | Preserved because the base digest and every downloaded JAR checksum are already verified in `infrastructure/docker/spark.Dockerfile` |

The local `TITAN_SPARK_IMAGE` default remains `titan-spark:4.0.1` because it is a developer-built image, not a registry dependency. Hosted full-pipeline and recovery workflows resolve the published GHCR image to an immutable digest before Compose starts it. This is an explicit local-development exception, not a protected CI image reference.

## Automated policy

`scripts/check_supply_chain.py` rejects mutable action references in every workflow, mutable direct images in the protected base Compose file, and mutable Python/orchestration/web Docker bases. It allows only the documented local Spark build-tag expression. The policy runs in the normal `ci` Python job and has a unit regression test.

Python installation uses frozen `uv.lock` resolution and the workflows use `uv sync --frozen`. The frontend uses the committed `package-lock.json` with `npm ci`. The MinIO and Spark recovery workflows retain their existing digest/checksum controls.

## CI permissions and artifact handling

Workflow-level permissions remain least-privilege: normal CI and disposable integration jobs request `contents: read`; package write is limited to the Spark image publishing workflow, while consumers request only `packages: read`. Artifact uploads are pinned to an immutable action commit. Recovery workflows validate downloaded artifacts before use and perform cleanup in `if: always()` steps.

## Scan evidence

- Supply-chain policy: passed.
- Python tests: 84 passed, 1 skipped.
- Ruff, format, and mypy: passed.
- Repository secret scan: passed.
- Frozen Python export with `pip-audit==2.10.1`: no reported vulnerabilities.
- `npm audit --omit=dev`: 0 vulnerabilities.
- Frontend tests: 10 passed; ESLint and TypeScript passed.
- Compose base, clone, and E2E configurations: rendered successfully.
- Official `rhysd/actionlint:latest` container: passed.

## Residual risks and exceptions

Digest pinning proves immutability of the selected image manifest, not that the upstream project or build provenance is trustworthy. Registry signatures, SBOM attestations, and continuous digest refresh automation are not yet enforced. The Spark Dockerfile downloads Maven artifacts from Maven Central and verifies SHA-1; a future hardening step should record stronger SHA-256 evidence where upstream publishes it. The MinIO recovery workflow intentionally uses its existing pinned image, while one disposable PostgreSQL workflow now uses the same verified PostgreSQL digest as the base stack.

The CI-only `pyarrow==26.0.1` install is a pinned but separately resolved test dependency rather than a project runtime dependency. It should eventually be moved into a lock-managed test group with hashes. Local development defaults and placeholder credentials remain unsuitable for shared or production deployment.

This work improves substitution resistance; it does not constitute production-grade supply-chain security by itself.
