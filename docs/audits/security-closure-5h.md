# TITAN Security Closure and Production Readiness — Upgrade 5H

**Assessment revision:** `e588a77af4ea2084e51c4353fb9a10042ee1af4e`  
**Scope:** security upgrades 5A–5G and their available local/hosted evidence.  
**Conclusion:** improved and suitable for a trusted workstation; not approved
for shared staging or network-accessible production.

## Executive assessment

Upgrades 5B–5G materially improved API authorization, loopback exposure,
default Dagster privilege, dependency pinning, backup artifact permissions, and
protected API secret configuration. The controls are real and covered by unit,
static, Compose, frontend, and CI checks. They do not provide a production
security boundary: local credentials remain predictable by design, Kafka and
service traffic remain plaintext, environment variables are used for secrets,
Windows ACL enforcement was not tested, and backup encryption/key management is
not implemented.

The latest hosted evidence available for `e588a77` shows CI and Kafka/PostgreSQL
integration passed. Coordinated recovery was still in progress at the time of
assessment, so no success is claimed for that workflow. Full Spark/Warehouse
verification was not triggered by the security-profile paths.

## Original finding resolution matrix

| Finding | Status | Evidence and remaining boundary |
|---|---|---|
| SEC-01 unauthenticated API | Resolved and verified for documented routes | Central auth middleware in `apps/api/src/titan_api/auth.py`; route policy tests in `tests/unit/test_auth.py` and `test_day6_policy.py`. Health is public; protected reads/writes/admin operations require scopes. |
| SEC-02 exposed services and predictable credentials | Partially mitigated / accepted local risk | Compose host bindings are loopback-bound and documented in `docker-compose.yml`; CI overrides are isolated. Local placeholders remain in `.env.example` and service defaults; Kafka is plaintext. Not suitable for shared or remote deployment. |
| SEC-03 mutable images/actions | Resolved and verified for repository policy | `scripts/check_supply_chain.py`, digest-pinned protected images, SHA-pinned actions, frozen lockfiles, and CI supply-chain checks. Registry signatures, SBOM attestations, and continuous refresh automation remain open. |
| SEC-04 backup confidentiality | Partially mitigated | `scripts/dr_security.py`, DR writers, and `umask 077` enforce POSIX `0700`/`0600`; Linux regression test passed. Windows ACLs were not tested. Archives remain unencrypted and CI retention/access remains an operational risk. |
| SEC-05 MinIO root runtime | Partially mitigated | Default Dagster has no socket; MinIO still uses `user: 0:0` for existing volume compatibility. Migration to non-root requires a planned volume procedure. |
| SEC-06 narrow secret scanner | Still open | `scripts/scan_secrets.py` passes but remains intentionally narrow. It is not a substitute for Gitleaks/TruffleHog or artifact scanning. |

## Security boundary review

- API: bearer tokens use read/write/admin scopes and timing-safe comparison; invalid and missing credentials fail closed. The profile validator in `apps/api/src/titan_api/config.py` rejects unknown profiles, normalizes case/whitespace, and rejects missing or placeholder protected secrets.
- Dashboard: `apps/web/src/app/api/[...path]/route.ts` keeps privileged tokens server-side, restricts upstream/path/method behavior, and requires the local-only boundary. No `NEXT_PUBLIC_*` privileged token is used.
- Network: base Compose services publish only loopback host ports; internal service DNS remains available. This is host-local isolation, not remote authentication or TLS.
- Dagster: the default profile has no Docker socket. `docker-compose.dagster-local.yml` remains an explicit root-equivalent opt-in and is not safe for untrusted/shared use.
- Backups: checksums, safe destinations, cleanup, and POSIX modes are implemented. Encryption at rest, key escrow, retention policy, and Windows ACL verification are not.
- CI/supply chain: actions and protected images are pinned; permissions are least privilege by workflow. Provenance/SBOM/signature enforcement is not complete.
- Observability: loopback exposure and authentication settings are development-oriented; production identity, TLS, and audit logging are not established.

## Deployment readiness

| Scenario | Readiness | Required assumptions or changes |
|---|---|---|
| Trusted single-developer workstation | Adequate with documented limits | Use loopback Compose, local-only dashboard, disposable/local credentials, and do not expose ports or share backups. The Dagster local socket override is trusted-user-only. |
| Shared internal development/staging | Not approved | Replace all defaults with managed secrets, add service authentication and TLS/SASL, remove the Dagster socket, use non-root MinIO, restrict networks, add identity-aware API access, and define backup encryption/retention. |
| Network-accessible production | Not approved | Requires all staging changes plus managed secret lifecycle, mTLS/TLS, Kafka SASL/TLS, database/object-store access controls, signed/SBOM-verified images, centralized audit logs/alerts, tested encrypted backups, and coordinated DR with a defined RPO/RTO. |

## Residual risk register

| Severity | Risk | Evidence |
|---|---|---|
| High | Plaintext service protocols and local default credentials become exploitable if exposed beyond the host | `docker-compose.yml` Kafka plaintext listeners and fallback credentials |
| High | Environment-variable secrets and CI artifact access can disclose credentials/data to privileged operators | Compose/workflow environment injection; no external secrets manager |
| High | Coordinated recovery and Windows confidentiality evidence are incomplete | Hosted coordinated run was pending; no Windows ACL test |
| Medium | Dagster local override grants root-equivalent Docker control | `docker-compose.dagster-local.yml` socket mount |
| Medium | MinIO runs as root for volume compatibility | `docker-compose.yml` MinIO `user: 0:0` |
| Medium | No authenticated encryption or managed backup-key lifecycle | DR documentation and scripts |
| Medium | Image/action provenance and SBOM attestations are not enforced | `docs/audits/supply-chain-security.md` |
| Low | Secret scanning coverage is narrow | `scripts/scan_secrets.py` |

## Ranked remediation roadmap

1. Establish a protected deployment profile using a managed secret store,
   TLS/SASL/mTLS, private networks, and no Docker socket; fail closed for every
   stateful service, not only the API.
2. Implement authenticated backup encryption with external key custody,
   rotation, retention/deletion controls, and tested restore-key procedures.
3. Complete coordinated recovery hosted verification and Windows ACL testing;
   define measured RPO/RTO and alerting for failed/partial publication.
4. Migrate MinIO to non-root operation without resetting existing volumes.
5. Add maintained secret and artifact scanning plus image signatures/SBOM
   verification and automated digest refresh review.

## Evidence and gaps

Local evidence includes the full non-integration Python suite, API auth/profile
tests, frontend tests/lint/typecheck, Ruff, mypy, secret scan, supply-chain
checker, Compose validation, and actionlint. Hosted evidence includes successful
CI and Kafka/PostgreSQL integration for `e588a77`; coordinated recovery was not
terminal-success at assessment time. No authenticated penetration test,
external network scan, Windows ACL test, TLS/SASL test, runtime escape test,
managed-secret-store test, or production deployment was performed.

## Non-goals and claims not made

This assessment does not claim production approval, encrypted backups, atomic
distributed snapshots, guaranteed RPO/RTO, secure remote deployment, complete
Windows enforcement, or elimination of the deliberately trusted local Dagster
socket override.
