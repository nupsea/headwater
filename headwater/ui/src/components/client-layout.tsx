"use client";

import React from "react";
import { ErrorBoundary } from "./error-boundary";
import { ToastProvider } from "./toast";

export function ClientLayout({ children }: { children: React.ReactNode }) {
  return (
    <ToastProvider>
      <ErrorBoundary>{children}</ErrorBoundary>
    </ToastProvider>
  );
}
