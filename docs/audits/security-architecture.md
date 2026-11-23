# TITAN Security Architecture Audit

**Audit date:** 2026-11-23  
**Repository revision:** `d104bdfae99f8c31ae0bf2af88ab3971813ccb2f`  
**Scope:** repository source, Docker Compose configuration, CI workflows, backup utilities, and available local checks.  
**Method:** read-only source/configuration review plus repository-provided scans. No exploit attempts, destructive tests, volume changes, or database changes were performed.

## Executive assessment

TITAN has useful development safeguards but is not production-secure as currently composed. The dominant risk is an unsafe deployment boundary: the default Compose stack publishes Kafka, PostgreSQL, ClickHouse, MinIO, the API, and the web UI on all host interfaces while using predictable development credentials. The API protects replay/resume operations with a bearer token, but most read endpoints and the primary ingestion/import write endpoints are unauthenticated. These are acceptable only when the stack is strictly local and the host is trusted; they become high-impact findings if the ports are reachable from another user, VLAN, cloud network, or an untrusted browser origin.

No Critical finding was verified from the inspected code. Two High findings require remediation before any non-local exposure. Several Medium findings increase blast radius or supply-chain uncertainty but are not by themselves proof of compromise.

## Threat model

Assets include commerce events and customer/order data in PostgreSQL, Kafka event history, MinIO Bronze/Silver objects, ClickHouse serving data, backup archives, publication metadata, administrative replay/import controls, and CI package/image credentials.

Assumed attackers are: (a) another host able to reach published Docker ports, (b) a malicious or compromised browser origin able to call the API, (c) a compromised dependency/image/action, and (d) a user who obtains a development credential or backup archive. The audit does not assume access to the Docker daemon or the GitHub organization.

Trust boundaries are the host-to-container published ports, browser-to-API CORS boundary, API-to-Kafka/PostgreSQL/ClickHouse/MinIO network, CI runner-to-GHCR, and backup archive filesystem boundary. Local-only Compose use is a compensating control, not an authentication boundary.

## Findings

### SEC-01 — High — Unauthenticated data-plane and ingestion API

**Affected locations:** `apps/api/src/titan_api/routes.py:70-196,210-325`; `apps/api/src/titan_api/main.py:286-331`.

**Evidence:** `POST /api/v1/ingestion/events`, `POST /api/v1/ingestion/imports`, import reads, submissions, quarantine, replay reads, analytics, monitoring, and recent-event routes are registered without an authentication dependency. `_require_admin()` is called only by import resume and replay creation (`routes.py:386-391`). The public event route publishes arbitrary valid events to Kafka and the import route accepts file uploads.

**Exploitability/impact:** Any caller who can reach the API can submit events, upload bounded CSV data, enumerate operational/financial data, and read quarantine/submission details. An attacker can create ingestion noise, alter downstream analytics, consume resources, and disclose event metadata. CORS is not an access-control mechanism; it only affects browser enforcement.

**Remediation:** Introduce an explicit authentication dependency for every non-health route, separate read and write scopes, and require authorization for ingestion, imports, operational data, analytics, quarantine, and replay. Prefer short-lived OIDC/JWT or a reverse-proxy identity layer; if a token is retained for local use, require it for all sensitive routes and rotate it. Add negative tests for anonymous and cross-tenant access.

**Compatibility/operational risk:** Existing dashboard calls and local integration tests will need configured credentials or a documented development-only bypass. Authentication must preserve the current 202 Kafka-acknowledgement semantics.

### SEC-02 — High — Default Compose publishes privileged data services with predictable credentials

**Affected locations:** `docker-compose.yml:39-40,77-83,102-107,137-142,233-234,269-270`; `.env.example:7-37`.

**Evidence:** Kafka, PostgreSQL, ClickHouse, MinIO, API, and web ports use host bindings without `127.0.0.1` prefixes. The defaults use `titan_dev_password` for PostgreSQL, ClickHouse, MinIO, and Grafana, and the example admin token is `local-demo-token`. Kafka is configured with plaintext listeners (`docker-compose.yml:45-47`).

**Exploitability/impact:** On a network-reachable workstation, an attacker may connect directly to databases/object storage or call the unauthenticated API. Plaintext Kafka permits interception or injection on the reachable listener. Predictable credentials make direct access practical. This can expose or alter all local data and backups.

**Remediation:** Make the secure posture the default: bind development ports to loopback, require explicitly supplied high-entropy secrets, and use TLS/SASL or a private network for any shared environment. Remove example tokens that look usable, or generate them at startup. Add a production Compose/profile that fails closed when secrets or TLS are absent and add a CI assertion that no service is externally bound without an explicit opt-in.

**Compatibility/operational risk:** Loopback binding changes access for remote developers and host-side tools. Kafka TLS/SASL changes client configuration and requires coordinated certificate/credential rotation.

### SEC-03 — Medium — Mutable image tags remain in the default runtime and several workflows

**Affected locations:** `docker-compose.yml:16,36,75,100,131,399,424,451,466,478,494`; `infrastructure/docker/python.Dockerfile:1`, `orchestration.Dockerfile:1`, `web.Dockerfile:1`; `.github/workflows/*.yml` action references.

**Evidence:** Most runtime images use version tags rather than immutable digests. The Spark Dockerfile is digest-pinned (`infrastructure/docker/spark.Dockerfile:4`) and the MinIO recovery workflow uses a digest, but the normal Compose services and Python/web base images are tag-pinned only. GitHub Actions use floating major tags such as `actions/checkout@v7`, `astral-sh/setup-uv@v7`, `docker/login-action@v3`, and `actions/upload-artifact@v7`.

**Exploitability/impact:** A registry compromise, tag retargeting, or future upstream change can alter the executed software without a source change. CI actions run with repository contents and, in the Spark publishing workflow, package write permission.

**Remediation:** Pin production images and third-party actions to verified commit/image digests, record provenance/SBOM evidence, and use Dependabot/Renovate to update pins. Keep the current Spark digest strategy as the model. Restrict package write permission to the image-publishing job and review action release provenance.

**Compatibility/operational risk:** Digest pinning requires an update process and can delay security patches if automation is absent. Action pinning increases maintenance overhead.

### SEC-04 — Medium — Backup archive confidentiality depends on host platform and operator umask

**Affected locations:** `scripts/postgres_dr.py:64-100`; `scripts/minio_dr.py:52-96`; `scripts/kafka_dr.py` backup output handling.

**Evidence:** PostgreSQL archive and manifest permissions are changed to `0600` only when `os.name != "nt"` (`postgres_dr.py:98-100`). MinIO and Kafka backups write payloads/manifests but do not enforce restrictive permissions in the script. Archives contain event data and analytical objects. The repository correctly ignores `.env` and `artifacts/`, and the secret scan passed, but filesystem permissions are not a cross-platform guarantee.

**Exploitability/impact:** A shared workstation, permissive temporary directory, CI artifact retention, or accidental archive upload can disclose commerce/customer data. Checksums provide integrity, not confidentiality.

**Remediation:** Require an explicit secure destination, apply restrictive permissions on Windows using an ACL-aware implementation, reject unsafe locations, encrypt archives at rest with an externally managed key, and define CI artifact retention/deletion rules. Ensure logs never print DSNs or archive contents.

**Compatibility/operational risk:** Encryption and ACL enforcement add key-management and restore prerequisites. Existing local recovery commands need a documented opt-in for plaintext development fixtures.

### SEC-05 — Medium — MinIO container runs as root inside the container

**Affected location:** `docker-compose.yml:131-134`.

**Evidence:** The MinIO service explicitly sets `user: "0:0"`. The comment explains this is to initialize a root-owned named volume; it is not Docker privileged mode, and no `privileged` or `cap_add` setting was found.

**Exploitability/impact:** A MinIO image or service-level vulnerability has a larger container-level impact, and a container escape would start from a root process. This is reduced by the absence of host mounts beyond the named data volume and by the local-development scope, but root is unnecessary after volume initialization.

**Remediation:** Initialize/chown the volume in a one-shot setup service, then run MinIO with a fixed non-root UID, read-only root filesystem where supported, dropped capabilities, and a writable data mount only.

**Compatibility/operational risk:** UID/GID changes can make existing development volumes unreadable. This must be implemented only with an explicit migration and must not reset current volumes.

### SEC-06 — Low — Security scanner coverage is narrow

**Affected location:** `scripts/scan_secrets.py:1-34`.

**Evidence:** The scanner checks only two PEM private-key header markers and one common AWS access-key prefix, and skips `benchmarks/` entirely. It does not detect generic passwords, bearer tokens, database URLs, other cloud tokens, or secrets embedded in JSON/YAML. Existing CI also relies on this script.

**Impact:** A secret can pass the repository check while still being committed or uploaded as benchmark evidence.

**Remediation:** Add a maintained detector such as Gitleaks/TruffleHog in CI, scan the Git diff and all tracked content, classify intentional placeholders, and scan workflow artifacts before publication. Do not treat benchmark files as automatically safe.

**Compatibility/operational risk:** Broader detection creates false positives in fixtures and documentation; use explicit test-only allowlists rather than weakening the scanner globally.

## Positive controls verified

- Admin-only import resume and replay creation fail closed when `TITAN_ADMIN_TOKEN` is absent or mismatched (`routes.py:386-391`).
- CORS uses an explicit configured origin list and disables credentialed browser requests (`main.py:139-144`). This is useful browser hygiene, not authentication.
- Liveness and readiness are separated; readiness checks migrated tables and dependencies.
- PostgreSQL restore restricts destinations to local hosts and names ending `_restore` or `_dr` and refuses nonempty destinations by default (`scripts/postgres_dr.py:54-61,203-220`).
- MinIO restore requires a restore-suffixed destination, verifies object checksums, cleans partial objects, and writes its completion marker after verification (`scripts/minio_dr.py:118-170`).
- CI workflows request `contents: read`; package write is limited to the Spark image publishing workflow. Integration projects use explicit non-development project names and cleanup steps.
- Prometheus, Grafana, and Tempo host ports are loopback-bound (`docker-compose.yml:409-410,436-437,457-458`); exporter services have no published host ports.
- `uv.lock` contains hashes, the Spark base image is digest-pinned, the repository secret scan passed, and Compose configuration rendered successfully.

## Validation performed

- `uv run python scripts/scan_secrets.py` — passed; coverage limitation documented in SEC-06.
- `docker compose config --quiet` — passed.
- `uv export --frozen ...` followed by `uvx --from pip-audit==2.10.1 pip-audit` — completed without reported vulnerabilities.
- `npm audit --omit=dev --prefix apps/web` — completed without reported vulnerabilities.
- Read-only source/configuration review of API routes, CORS, Compose, Dockerfiles, backup utilities, workflows, frontend API calls, observability configuration, and Git ignore rules.

Not performed: authenticated penetration testing, external network scanning, registry signature/provenance verification for every image, runtime container escape testing, TLS/SASL validation, and production secret-manager review. No conclusion about production security can be drawn from these omissions.

## Prioritized remediation roadmap

1. Before any shared or production-like deployment: bind data services to private/loopback interfaces, replace defaults with managed secrets, enable Kafka/database/object-store transport security, and enforce API authentication/authorization (SEC-01, SEC-02).
2. Protect backup archives with encryption, ACLs, retention controls, and artifact redaction (SEC-04).
3. Establish immutable image/action pinning with provenance and SBOM verification (SEC-03).
4. Remove the MinIO root runtime requirement through a volume-initialization migration (SEC-05).
5. Replace the narrow secret scanner with a maintained detector and artifact scan (SEC-06).
6. Add security regression tests for anonymous API access, default-secret refusal, network binding, archive permissions, and CI permission boundaries.

## Conclusion

The repository is suitable for controlled local development and has meaningful recovery/CI isolation controls. It should be treated as **not approved for network-exposed or production use** until SEC-01 and SEC-02 are addressed and the image, secret, and backup controls are strengthened. This audit intentionally makes no broad infrastructure changes.
