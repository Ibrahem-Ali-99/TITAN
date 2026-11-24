import { createHash, createHmac, randomBytes } from "node:crypto";

import { jwtVerify, createRemoteJWKSet, type JWTPayload } from "jose";

export type TitanRole = "admin" | "operator" | "analyst" | "viewer";
export type TitanSession = { sub: string; roles: TitanRole[]; exp: number };

const cookieName = "titan_session";
const stateCookie = "titan_oidc_state";

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`missing protected OIDC configuration: ${name}`);
  return value;
}

function config() {
  return {
    issuer: required("TITAN_OIDC_ISSUER").replace(/\/$/, ""),
    clientId: required("TITAN_OIDC_CLIENT_ID"),
    clientSecret: required("TITAN_OIDC_CLIENT_SECRET"),
    redirectUri: required("TITAN_OIDC_REDIRECT_URI"),
    audience: required("TITAN_OIDC_AUDIENCE"),
    sessionSecret: required("TITAN_OIDC_SESSION_SECRET"),
  };
}

function b64(value: Uint8Array | string): string {
  return Buffer.from(value).toString("base64url");
}

function sign(value: string, secret: string): string {
  return b64(createHmac("sha256", secret).update(value).digest());
}

export function oidcEnabled(): boolean {
  return process.env.TITAN_SECURITY_PROFILE !== "local";
}

export function sessionCookieName(): string {
  return cookieName;
}

export async function beginLogin(): Promise<{ location: string; state: string }> {
  const cfg = config();
  const discovery = await fetch(`${cfg.issuer}/.well-known/openid-configuration`, { cache: "no-store" });
  if (!discovery.ok) throw new Error("OIDC discovery failed");
  const metadata = (await discovery.json()) as { authorization_endpoint?: string };
  if (!metadata.authorization_endpoint) throw new Error("OIDC authorization endpoint missing");
  const state = b64(randomBytes(32));
  const nonce = b64(randomBytes(32));
  const verifier = b64(randomBytes(48));
  const challenge = b64(createHash("sha256").update(verifier).digest());
  const value = `${state}.${nonce}.${verifier}.${sign(`${state}.${nonce}.${verifier}`, cfg.sessionSecret)}`;
  const url = new URL(metadata.authorization_endpoint);
  url.searchParams.set("client_id", cfg.clientId);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", "openid profile email");
  url.searchParams.set("redirect_uri", cfg.redirectUri);
  url.searchParams.set("state", state);
  url.searchParams.set("nonce", nonce);
  url.searchParams.set("code_challenge", challenge);
  url.searchParams.set("code_challenge_method", "S256");
  return { location: url.toString(), state: value };
}

function roles(payload: JWTPayload): TitanRole[] {
  const raw = payload["titan_roles"] ?? payload["roles"];
  const values = Array.isArray(raw) ? raw : typeof raw === "string" ? raw.split(" ") : [];
  const accepted = new Set<TitanRole>(["admin", "operator", "analyst", "viewer"]);
  const result = values.filter((role): role is TitanRole => typeof role === "string" && accepted.has(role as TitanRole));
  return result.length ? [...new Set(result)] : ["viewer"];
}

export async function exchangeCallback(code: string, stateValue: string, returnedState: string): Promise<TitanSession> {
  const cfg = config();
  const [state, nonce, verifier, mac] = stateValue.split(".");
  if (!state || !nonce || !verifier || mac !== sign(`${state}.${nonce}.${verifier}`, cfg.sessionSecret) || state !== returnedState) {
    throw new Error("invalid OIDC state");
  }
  const discovery = await fetch(`${cfg.issuer}/.well-known/openid-configuration`, { cache: "no-store" });
  if (!discovery.ok) throw new Error("OIDC discovery failed");
  const metadata = (await discovery.json()) as { token_endpoint?: string; jwks_uri?: string };
  if (!metadata.token_endpoint || !metadata.jwks_uri) throw new Error("OIDC token metadata incomplete");
  const body = new URLSearchParams({ grant_type: "authorization_code", code, redirect_uri: cfg.redirectUri, client_id: cfg.clientId, client_secret: cfg.clientSecret, code_verifier: verifier });
  const tokenResponse = await fetch(metadata.token_endpoint, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body, cache: "no-store" });
  if (!tokenResponse.ok) throw new Error("OIDC code exchange failed");
  const tokens = (await tokenResponse.json()) as { id_token?: string };
  if (!tokens.id_token) throw new Error("OIDC response did not include an ID token");
  const jwks = createRemoteJWKSet(new URL(metadata.jwks_uri));
  const verified = await jwtVerify(tokens.id_token, jwks, { issuer: cfg.issuer, audience: cfg.audience });
  if (verified.payload.nonce !== nonce || typeof verified.payload.sub !== "string") throw new Error("invalid OIDC nonce or subject");
  const exp = Math.min((verified.payload.exp ?? 0) * 1000, Date.now() + 8 * 60 * 60 * 1000);
  if (exp <= Date.now()) throw new Error("OIDC session is expired");
  return { sub: verified.payload.sub, roles: roles(verified.payload), exp };
}

export function encodeSession(session: TitanSession): string {
  const cfg = config();
  const payload = b64(JSON.stringify(session));
  return `${payload}.${sign(payload, cfg.sessionSecret)}`;
}

export function decodeSession(value: string | undefined): TitanSession | null {
  try {
    if (!value) return null;
    const [payload, mac] = value.split(".");
    const cfg = config();
    if (!payload || mac !== sign(payload, cfg.sessionSecret)) return null;
    const session = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as TitanSession;
    return session.exp > Date.now() && typeof session.sub === "string" ? session : null;
  } catch {
    return null;
  }
}

export { cookieName, stateCookie };
