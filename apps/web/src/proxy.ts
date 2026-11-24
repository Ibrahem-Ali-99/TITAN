import { NextRequest, NextResponse } from "next/server";

import { decodeEdgeSession } from "@/lib/session-edge";

const cookieName = "titan_session";

export async function proxy(request: NextRequest) {
  if (process.env.TITAN_SECURITY_PROFILE === "local") return NextResponse.next();
  if (await decodeEdgeSession(request.cookies.get(cookieName)?.value)) return NextResponse.next();
  const login = new URL("/auth/login", request.url);
  login.searchParams.set("returnTo", request.nextUrl.pathname);
  return NextResponse.redirect(login);
}

export const config = {
  matcher: ["/((?!auth|_next/static|_next/image|favicon.ico).*)"],
};
