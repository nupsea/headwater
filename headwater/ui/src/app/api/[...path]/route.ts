import { NextRequest, NextResponse } from "next/server";

const API_URL = process.env.API_URL || "http://localhost:8000";

// Allow long-running backend operations (LLM enrichment, pipeline runs)
export const maxDuration = 300; // 5 minutes

async function proxy(req: NextRequest): Promise<NextResponse> {
  const url = new URL(req.url);
  const target = `${API_URL}/api${url.pathname.replace(/^\/api/, "")}${url.search}`;

  const headers = new Headers(req.headers);
  headers.delete("host");

  // Buffer the request body to avoid streaming issues with some Next.js versions
  let body: ArrayBuffer | undefined;
  if (req.method !== "GET" && req.method !== "HEAD") {
    body = await req.arrayBuffer();
  }

  try {
    const upstream = await fetch(target, {
      method: req.method,
      headers,
      body,
      signal: AbortSignal.timeout(300_000), // 5 min timeout for LLM ops
    });

    return new NextResponse(upstream.body, {
      status: upstream.status,
      headers: upstream.headers,
    });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Backend connection failed";
    if (message.includes("timeout") || message.includes("abort")) {
      return NextResponse.json(
        { detail: "Request timed out. The operation is still running on the backend." },
        { status: 504 }
      );
    }
    return NextResponse.json(
      { detail: `Proxy error: ${message}. Is the backend running on ${API_URL}?` },
      { status: 502 }
    );
  }
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
