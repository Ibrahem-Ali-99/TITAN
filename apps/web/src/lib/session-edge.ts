export type EdgeSession = { sub: string; roles: string[]; exp: number };

function encoded(value: string): Uint8Array {
  return Uint8Array.from(atob(value.replace(/-/g, "+").replace(/_/g, "/")), (char) => char.charCodeAt(0));
}

function b64(bytes: ArrayBuffer): string {
  return btoa(String.fromCharCode(...new Uint8Array(bytes))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export async function decodeEdgeSession(value: string | undefined): Promise<EdgeSession | null> {
  try {
    if (!value || !process.env.TITAN_OIDC_SESSION_SECRET) return null;
    const [payload, mac] = value.split(".");
    if (!payload || !mac) return null;
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(process.env.TITAN_OIDC_SESSION_SECRET), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const expected = b64(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload)));
    if (expected !== mac) return null;
    const session = JSON.parse(new TextDecoder().decode(encoded(payload))) as EdgeSession;
    return typeof session.sub === "string" && session.exp > Date.now() ? session : null;
  } catch {
    return null;
  }
}
