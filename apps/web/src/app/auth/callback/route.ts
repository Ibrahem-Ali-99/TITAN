import { NextRequest, NextResponse } from "next/server";

import { cookieName, encodeSession, exchangeCallback, oidcEnabled, stateCookie } from "@/lib/oidc";

export async function GET(request: NextRequest) {
  if (!oidcEnabled()) return NextResponse.json({ detail: "OIDC is disabled in local mode" }, { status: 404 });
  const code = request.nextUrl.searchParams.get("code");
  const state = request.nextUrl.searchParams.get("state");
  const stored = request.cookies.get(stateCookie)?.value;
  if (!code || !state || !stored) return NextResponse.json({ detail: "invalid OIDC callback" }, { status: 400 });
  try {
    const session = await exchangeCallback(code, stored, state);
    const response = NextResponse.redirect(new URL("/", request.url));
    response.cookies.set(cookieName, encodeSession(session), { httpOnly: true, secure: true, sameSite: "lax", maxAge: 8 * 60 * 60, path: "/" });
    response.cookies.delete(stateCookie);
    return response;
  } catch {
    return NextResponse.json({ detail: "OIDC authentication failed" }, { status: 401 });
  }
}
