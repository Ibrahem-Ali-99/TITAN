import { NextRequest, NextResponse } from "next/server";
import { randomUUID } from "node:crypto";

import { cookies } from "next/headers";
import { cookieName, decodeSession, oidcEnabled } from "@/lib/oidc";

const upstream = process.env.TITAN_API_INTERNAL_URL ?? "http://api:8000";
const allowedPaths = ["/health/ready", "/v1/"];

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
  const requestId = randomUUID();
  const started = Date.now();
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
  const upstreamPath = pathname.startsWith("/v1/") ? `/api${pathname}` : pathname;
  const target = `${upstreamUrl.origin}${upstreamPath}${request.nextUrl.search}`;
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
  const signal = AbortSignal.any([request.signal, AbortSignal.timeout(10_000)]);
  let responseHeadersReceived = false;
  try {
    const response = await fetch(target, {
      method: request.method,
      headers,
      body: request.method === "GET" ? undefined : await request.arrayBuffer(),
      cache: "no-store",
      signal,
    });
    responseHeadersReceived = true;
    const body = await response.arrayBuffer();
    return new NextResponse(body, { status: response.status, headers: response.headers });
  } catch (error) {
    const detail = error as {
      name?: unknown;
      message?: unknown;
      code?: unknown;
      cause?: { name?: unknown; code?: unknown; message?: unknown };
    };
    console.error(
      "[titan-proxy-error]",
      JSON.stringify({
        request_id: requestId,
        name: typeof detail.name === "string" ? detail.name : typeof error,
        message: typeof detail.message === "string" ? detail.message.slice(0, 240) : "unknown",
        code: typeof detail.code === "string" ? detail.code : null,
        cause_name: typeof detail.cause?.name === "string" ? detail.cause.name : null,
        cause_code: typeof detail.cause?.code === "string" ? detail.cause.code : null,
        cause_message: typeof detail.cause?.message === "string" ? detail.cause.message.slice(0, 240) : null,
        aborted: signal.aborted,
        elapsed_ms: Date.now() - started,
        upstream_path: upstreamPath,
        response_headers_received: responseHeadersReceived,
      }),
    );
    if (error instanceof DOMException && error.name === "AbortError") {
      return NextResponse.json({ detail: "dashboard API upstream timed out" }, { status: 504 });
    }
    return NextResponse.json({ detail: "dashboard API upstream unavailable" }, { status: 502 });
  }
}
