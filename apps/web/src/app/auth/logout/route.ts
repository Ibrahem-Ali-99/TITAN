import { NextRequest, NextResponse } from "next/server";

import { cookieName } from "@/lib/oidc";

export async function POST(request: NextRequest) {
  const origin = request.headers.get("origin");
  const publicOrigin = new URL(process.env.TITAN_OIDC_REDIRECT_URI!).origin;
  if (origin && origin !== publicOrigin) {
    return NextResponse.json({ detail: "cross-origin request rejected" }, { status: 403 });
  }
  const response = NextResponse.json({ ok: true });
  response.cookies.set(cookieName, "", { httpOnly: true, secure: true, sameSite: "lax", maxAge: 0, path: "/" });
  return response;
}
