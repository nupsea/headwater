import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // API proxying is handled by src/app/api/[...path]/route.ts
  // which provides timeout control and proper error handling.
  // Do NOT add rewrites for /api/* -- they conflict with the route handler.
};

export default nextConfig;
