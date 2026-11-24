# OIDC and dashboard RBAC

Protected staging now requires an external OIDC provider. The Next.js server
uses Authorization Code Flow with PKCE, state, nonce, exact issuer/audience
validation, and a server-only client secret. ID tokens are verified against
the issuer JWKS using `jose`; the browser never receives provider tokens or
the internal FastAPI bearer tokens.

Required protected-staging settings are supplied only at runtime:

```text
TITAN_OIDC_ISSUER
TITAN_OIDC_CLIENT_ID
TITAN_OIDC_CLIENT_SECRET
TITAN_OIDC_REDIRECT_URI
TITAN_OIDC_AUDIENCE
TITAN_OIDC_SESSION_SECRET
```

The redirect URI must exactly match the provider registration and the HTTPS
ingress origin. Missing configuration or an unavailable provider fails closed;
there is no anonymous protected-mode fallback. Sessions are signed, expire no
later than the ID-token expiry or eight hours, and use HTTP-only, Secure,
SameSite=Lax cookies. Logout clears the session cookie.

Roles are read only from verified ID-token claims (`titan_roles`, then
`roles`), never from browser headers or query parameters:

| Role | Read dashboard data | Write operations | Administrative publish/replay |
| --- | --- | --- | --- |
| viewer | yes | no | no |
| analyst | yes | no | no |
| operator | yes | yes | no |
| admin | yes | yes | yes |

The server-side proxy maps these permissions to the existing internal
read/write/admin bearer tokens. The FastAPI contract remains unchanged for
service-to-service callers. Direct API access still requires its bearer token.

Local development remains token/local-only and does not require an identity
provider. Shared staging and public deployment still require provider-managed
client registration, TLS, user lifecycle, audit logging, and a disposable IdP
integration test before approval.
