"use client";

import { usePathname } from "next/navigation";
import { AppTopbar } from "@/components/app-topbar";
import { AppSidebar } from "@/components/app-sidebar";
import { RerunBanner } from "@/components/rerun-banner";
import { ClientLayout } from "@/components/client-layout";

export function H1Shell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  if (pathname.startsWith("/h2")) {
    // H2 routes have their own shell — render children naked
    return <>{children}</>;
  }

  return (
    <ClientLayout>
      <AppTopbar />
      <div className="flex flex-1 overflow-hidden">
        <AppSidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="mx-auto w-full max-w-[min(1800px,100vw)] px-6 py-6">
            <RerunBanner />
            {children}
          </div>
        </main>
      </div>
    </ClientLayout>
  );
}
