import { NextRequest, NextResponse } from "next/server";

const API_URL = process.env.API_URL || "http://localhost:8000";

async function proxy(req: NextRequest): Promise<NextResponse> {
  const url = new URL(req.url);
  const target = `${API_URL}/api${url.pathname.replace(/^\/api/, "")}${url.search}`;

  const headers = new Headers(req.headers);
  headers.delete("host");

  const upstream = await fetch(target, {
    method: req.method,
    headers,
    body: req.method !== "GET" && req.method !== "HEAD" ? req.body : undefined,
    // @ts-expect-error -- Node fetch duplex required for streaming bodies
    duplex: "half",
  });

  return new NextResponse(upstream.body, {
    status: upstream.status,
    headers: upstream.headers,
  });
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
