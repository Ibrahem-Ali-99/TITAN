# TITAN release checkpoint

Recorded 26 November 2026, Cairo time. Release status is **blocked**.

## Candidate and changes

The existing Windows checkout is `D:\TITAN`, branch `master`. It started
clean at `c29c792de7c876a7fa92b2309ab796a2ea33236a`, 23 commits ahead of
GitHub `master` at `3f529dbc498c0647e96cdff3183ebaee5de3cb5b`. All existing
commits are preserved. The changes below are present in the working tree.
Git could not create `.git/index.lock` in this session.

- Monitoring reads derive durations for legacy pipeline runs from their
  timestamps. Stored durations and unfinished runs retain their original
  meaning, and monitoring reads leave run history untouched.
- CSV upload and resume distinguish permissions from unconfirmed outcomes.
  The proxy recognizes its request deadline and reports HTTP 504 when it
  expires.
- The chained full-pipeline and coordinated recovery workflows check out
  the same source SHA as the successful Spark image build. They restrict
  chained runs to this repository's `master` branch.
- The protected browser harness writes evidence under ignored `artifacts/`,
  validates per-role results, and keeps raw Chromium traffic out of its
  output. Setup documentation includes separate navigation and RBAC runs.
- Reproduction commands use module execution where the scripts import
  other modules from the repository's `scripts` package.

## Acceptance evidence

| Area | Evidence |
| --- | --- |
| Python | 115 passed, 5 skipped, 1 integration test deselected. Ruff lint and format passed. Mypy reported zero issues across 135 files. |
| Frontend | 11 tests passed. ESLint, TypeScript, and the Next.js production build passed. |
| Browser fixture | Production HTML and assets loaded in Chromium. All eight routes at 1440, 1024, 768, and 390 px passed automated axe 4.14.1 WCAG 2 A/AA, viewport overflow, and page error checks with intercepted HTTP 503 API responses. The data sources page also passed fixture checks for empty imports, row errors, upload and resume outcomes. |
| Local configuration | Base, clone, and e2e Compose configurations parsed. All eight workflow YAML files parsed, including the two chained checkout assertions. Secret scan and immutable supply-chain reference checks passed. A format-based check found no private-key or provider-token matches in 102 new historical blobs. |
| Full data pipeline | [End-to-end reliability](end-to-end-reliability.md), [Spark crash recovery](spark-crash-recovery.md), and [isolated install](clean-clone-release.md) record historical runs of Kafka ingestion and replay, Bronze and Silver, dbt and warehouse promotion, reconciliation, Dagster, and ML. |
| Coordinated recovery | [Coordinated recovery audit](coordinated-disaster-recovery.md) and [restore evidence](../../benchmarks/coordinated-restore.json) record the isolated artifact restore, duplicate replay handling, warehouse rebuild and negative checks. |

The Docker Desktop Linux engine pipe was denied. The final tree's live
integration, protected ingress and certificates, Keycloak, live browser
interactions, service latency, coordinated recovery, and clean install
remain unverified in this session. An admin receiving HTTP 404 on an unknown
import establishes that authorization allowed the request to reach the
handler. A successful resume against a real disposable import and its
durable downstream effects still need evidence. The historical reports and
mocked browser responses cover separate evidence.

The npm advisory service and GitHub Git transport failed DNS resolution.
The connected GitHub account can read repository metadata and reports no
push permission. Live Python dependency advisories and actionlint were not
run. No candidate commit was pushed and hosted workflows and release remain
pending.

## Resume from this working tree

1. Open the existing checkout in a session with write access to Git metadata,
   access to the Docker Desktop Linux engine, and network access for the
   configured package registries and GitHub. Confirm `git status -sb` and
   review the pending diff before staging.
2. Use isolated projects to run the integration, protected orchestration,
   protected browser and coordinated recovery procedures. The browser
   commands are in the [protected staging guide](../security/staging-deployment.md).
   Exercise CSV upload and resume against a real disposable import, and
   use the isolated e2e workflow to verify replay persistence. Review actual
   loaded and unavailable states at desktop and mobile widths. Reuse the
   historical evidence where it still covers unchanged code.
3. Run the frozen dependency advisories and actionlint. Review the 23
   unpublished commits and pending changes for secrets and generated
   artifacts before publishing a public repository. Commit the verified
   changes. Fetch and confirm that the push to `master` is a fast-forward.
4. Publish through the ordinary GitHub permissions and monitor the mandatory
   workflows. Confirm that the chained Spark integration results refer to
   the candidate SHA. Dispatch the Spark image workflow for `master` if its
   path filter does not start it for the final candidate. Resolve any failing
   gates and verify the successful workflow links.
5. Create and verify the release tag and GitHub release only after all gates
   pass. Confirm local and remote synchronization and a clean working tree.

TITAN targets a local or protected single-host staging deployment. Its
ingestion contract is at-least-once delivery with idempotent persistence.

## Verification update — 26 November 2026

The local release pass continued after this checkpoint. Docker Desktop access
was restored and the following disposable validations passed:

- Kafka/PostgreSQL/API ingestion: 1 integration test passed before and after
  restarting the ingestion consumer.
- Full Spark and serving pipeline: 256 events persisted idempotently; Bronze,
  Silver, warehouse publication, reconciliation, ClickHouse rebuild, and
  coordinated recovery passed. Spark regression tests passed 11/11.
- Protected staging: TLS, OIDC, browser navigation, and all four RBAC roles
  passed. Viewer and analyst writes returned 403, operator writes returned 202
  with admin actions denied, and admin writes/admin routing returned 202/404.
- Local quality: Python 115 passed and 6 skipped; Ruff, format, Mypy, frontend
  tests, ESLint, TypeScript, production build, secret scan, supply-chain check,
  Compose config validation, and actionlint passed.

The ClickHouse recovery helper was corrected to invoke `scripts.run_pipeline`
as a module, and the protected browser harness now retries the web loopback
readiness probe for bounded Next.js startup time. The remaining release block
is external: GitHub transport/authentication is unavailable in this session,
and registry advisory queries require explicit outbound-data approval. No
hosted workflow or release tag has been claimed from local evidence.
