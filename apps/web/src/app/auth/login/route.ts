import { NextResponse } from "next/server";

import { beginLogin, oidcEnabled, stateCookie } from "@/lib/oidc";

export async function GET() {
  if (!oidcEnabled()) return NextResponse.json({ detail: "OIDC is disabled in local mode" }, { status: 404 });
  try {
    const { location, state } = await beginLogin();
    const response = NextResponse.redirect(location);
    response.cookies.set(stateCookie, state, { httpOnly: true, secure: true, sameSite: "lax", maxAge: 600, path: "/" });
    return response;
  } catch {
    return NextResponse.json({ detail: "identity provider is unavailable" }, { status: 503 });
  }
}
