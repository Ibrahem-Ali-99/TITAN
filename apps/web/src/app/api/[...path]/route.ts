import { NextRequest, NextResponse } from "next/server";

const upstream = process.env.TITAN_API_INTERNAL_URL ?? "http://api:8000";

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
  const target = `${upstream}/${path.join("/")}${request.nextUrl.search}`;
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("authorization");
  const adminOperation = request.method === "POST" && (path.at(-1) === "publish" || path.at(-1) === "replays");
  if (adminOperation) {
    return NextResponse.json({ detail: "administrative actions require an authenticated operator client" }, { status: 403 });
  }
  const token = request.method === "POST" ? process.env.TITAN_WRITE_TOKEN : process.env.TITAN_READ_TOKEN;
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
