import { NextRequest, NextResponse } from "next/server";

import { cookies } from "next/headers";
import { cookieName, decodeSession, oidcEnabled } from "@/lib/oidc";

const upstream = process.env.TITAN_API_INTERNAL_URL ?? "http://api:8000";
const allowedPaths = ["/health/ready", "/api/v1/"];

export async function GET(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return forward(request, context);
}

export async function POST(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return forward(request, context);
}

async function forward(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  const { path } = await context.params;
  if (process.env.TITAN_DASHBOARD_LOCAL_ONLY !== "true" && !oidcEnabled()) {
    return NextResponse.json({ detail: "dashboard proxy is disabled outside local mode" }, { status: 503 });
  }
  const pathname = `/${path.join("/")}`;
  if (!allowedPaths.some((prefix) => pathname === prefix || pathname.startsWith(prefix))) {
    return NextResponse.json({ detail: "dashboard path is not allowed" }, { status: 404 });
  }
  const upstreamUrl = new URL(upstream);
  if (upstreamUrl.protocol !== "http:" || !["api", "localhost"].includes(upstreamUrl.hostname)) {
    return NextResponse.json({ detail: "dashboard upstream is not allowed" }, { status: 503 });
  }
  const target = `${upstreamUrl.origin}${pathname}${request.nextUrl.search}`;
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("authorization");
  if (oidcEnabled() && request.method !== "GET") {
    const origin = request.headers.get("origin");
    const publicOrigin = new URL(process.env.TITAN_OIDC_REDIRECT_URI!).origin;
    if (origin && origin !== publicOrigin) {
      return NextResponse.json({ detail: "cross-origin request rejected" }, { status: 403 });
    }
  }
  const adminOperation = request.method === "POST" && (path.at(-1) === "publish" || path.at(-1) === "replays");
  const session = oidcEnabled() ? decodeSession((await cookies()).get(cookieName)?.value) : null;
  const roles = new Set(session?.roles ?? []);
  if (oidcEnabled() && !session) {
    return NextResponse.json({ detail: "authentication required" }, { status: 401 });
  }
  if (adminOperation && oidcEnabled() && !roles.has("admin")) {
    return NextResponse.json({ detail: "administrative actions require an authenticated operator client" }, { status: 403 });
  }
  if (request.method !== "GET" && oidcEnabled() && !roles.has("admin") && !roles.has("operator")) {
    return NextResponse.json({ detail: "insufficient scope" }, { status: 403 });
  }
  const token = adminOperation
    ? process.env.TITAN_ADMIN_TOKEN
    : request.method === "POST"
      ? process.env.TITAN_WRITE_TOKEN
      : process.env.TITAN_READ_TOKEN;
  if (!token) {
    return NextResponse.json({ detail: "dashboard API authentication is not configured" }, { status: 503 });
  }
  headers.set("authorization", `Bearer ${token}`);
  const response = await fetch(target, {
    method: request.method,
    headers,
    body: request.method === "GET" ? undefined : await request.arrayBuffer(),
    cache: "no-store",
  });
  return new NextResponse(response.body, { status: response.status, headers: response.headers });
}
