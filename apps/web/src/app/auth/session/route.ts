import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import { cookieName, decodeSession, oidcEnabled } from "@/lib/oidc";

export async function GET() {
  if (!oidcEnabled()) return NextResponse.json({ authenticated: false, localMode: true });
  const session = decodeSession((await cookies()).get(cookieName)?.value);
  return NextResponse.json(session ? { authenticated: true, sub: session.sub, roles: session.roles, exp: session.exp } : { authenticated: false }, { headers: { "cache-control": "no-store" } });
}
