# TITAN secrets management

TITAN currently uses environment injection for local Compose and disposable CI.
This is a configuration boundary, not a secrets manager: values can be visible
to operators with access to the Docker daemon, process metadata, or CI logs.

## Profiles

The API accepts `TITAN_SECURITY_PROFILE=local` (the default) for the documented
loopback-only workstation stack. `protected`, `production`, and `nonlocal` are
fail-closed profiles. They reject missing or placeholder values for
`TITAN_READ_TOKEN`, `TITAN_WRITE_TOKEN`, `TITAN_ADMIN_TOKEN`, and
`CLICKHOUSE_PASSWORD`.

CI uses disposable credentials in isolated Compose projects and is not a
production secret boundary. Existing development database credentials are not
rotated automatically because changing them would require a volume migration.

## Initial setup

Copy `.env.example` to the ignored `.env` for local development. Generate
operator secrets with a platform CSPRNG, for example:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

For a protected deployment, set `TITAN_SECURITY_PROFILE=protected` and provide
unique values through an approved secret store or protected environment. Never
commit `.env`, paste values into workflow YAML, or expose privileged values as
`NEXT_PUBLIC_*` variables.

## Replacement and rollback

1. Generate replacement read, write, and admin tokens independently.
2. Update the protected secret source during a maintenance window.
3. Recreate only the API/dashboard consumers and verify 401/403 behavior.
4. Revoke the old values at the source of truth after verification.
5. Roll back by restoring the previous secret version if health checks fail.

Database, Kafka, ClickHouse, and MinIO credential rotation is deliberately
manual. Rotate those credentials only with a tested service-by-service
procedure, preserving existing volumes and recovery access. Backup encryption
keys, if introduced later, must be externally managed and backed up separately;
this phase does not add encryption.

## Emergency response and limitations

Revoke exposed API tokens immediately, disable the affected profile or service,
inspect CI/Docker logs, and issue replacement credentials. Environment variables
and Docker Compose interpolation can still expose secrets to privileged local
operators. Windows users must rely on protected environment-file ACLs rather
than assuming filesystem mode bits. Production use still requires a managed
secret store, TLS, rotation ownership, audit logging, and tested revocation.
