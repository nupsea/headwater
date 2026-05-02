"use client";

import React from "react";
import { ErrorBoundary } from "./error-boundary";
import { ToastProvider } from "./toast";
import { ProjectProvider } from "@/lib/project-context";

export function ClientLayout({ children }: { children: React.ReactNode }) {
  return (
    <ToastProvider>
      <ErrorBoundary>
        <ProjectProvider>{children}</ProjectProvider>
      </ErrorBoundary>
    </ToastProvider>
  );
}
