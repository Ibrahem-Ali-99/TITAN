import { createServer, type Server } from "node:http";
import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import { exportJWK, generateKeyPair, SignJWT } from "jose";

import { beginLogin, decodeSession, encodeSession, exchangeCallback } from "./oidc.ts";

let server: Server;
let issuer: string;
let nonce = "";

before(async () => {
  const { privateKey, publicKey } = await generateKeyPair("RS256");
  const jwk = await exportJWK(publicKey);
  server = createServer(async (request, response) => {
    if (request.url === "/.well-known/openid-configuration") {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ authorization_endpoint: `${issuer}/authorize`, token_endpoint: `${issuer}/token`, jwks_uri: `${issuer}/jwks` }));
      return;
    }
    if (request.url === "/jwks") {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ keys: [{ ...jwk, kid: "test" }] }));
      return;
    }
    if (request.url === "/authorize") {
      response.statusCode = 302;
      response.setHeader("location", "https://staging.local:8443/auth/callback?code=test-code&state=test-state");
      response.end();
      return;
    }
    if (request.url === "/token") {
      const token = await new SignJWT({ nonce, titan_roles: ["analyst"] }).setProtectedHeader({ alg: "RS256", kid: "test" }).setIssuer(issuer).setAudience("titan").setSubject("user-1").setExpirationTime("5m").sign(privateKey);
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ id_token: token }));
      return;
    }
    response.statusCode = 404;
    response.end();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert(address && typeof address === "object");
  issuer = `http://127.0.0.1:${address.port}`;
  process.env.TITAN_SECURITY_PROFILE = "protected";
  process.env.TITAN_OIDC_ISSUER = issuer;
  process.env.TITAN_OIDC_CLIENT_ID = "titan-test";
  process.env.TITAN_OIDC_CLIENT_SECRET = "server-only-secret";
  process.env.TITAN_OIDC_REDIRECT_URI = "https://staging.local:8443/auth/callback";
  process.env.TITAN_OIDC_AUDIENCE = "titan";
  process.env.TITAN_OIDC_SESSION_SECRET = "test-session-secret-long-enough";
});

after(() => server.close());

test("OIDC PKCE exchange verifies issuer, audience, nonce, signature, and roles", async () => {
  const login = await beginLogin();
  const url = new URL(login.location);
  assert.equal(url.searchParams.get("code_challenge_method"), "S256");
  assert.equal(url.searchParams.get("client_secret"), null);
  nonce = login.state.split(".")[1];
  const session = await exchangeCallback("test-code", login.state, login.state.split(".")[0]);
  assert.deepEqual(session.roles, ["analyst"]);
  assert.equal(session.sub, "user-1");
  const encoded = encodeSession(session);
  assert.deepEqual(decodeSession(encoded)?.roles, ["analyst"]);
  assert.equal(decodeSession(`${encoded}x`), null);
});
