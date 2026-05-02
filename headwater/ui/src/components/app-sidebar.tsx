"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  api,
  type ProjectProgress,
  type StatusResponse,
} from "@/lib/api";
import { useProjects } from "@/lib/project-context";

const NAV_GROUPS: {
  label: string;
  items: { href: string; label: string }[];
}[] = [
  {
    label: "Today",
    items: [
      { href: "/", label: "Briefing" },
      { href: "/projects", label: "Projects" },
      { href: "/health", label: "Project Health" },
    ],
  },
  {
    label: "Catalog",
    items: [
      { href: "/sources", label: "Sources" },
      { href: "/discovery", label: "Discover & Access" },
    ],
  },
  {
    label: "Build",
    items: [
      { href: "/models", label: "Model" },
      { href: "/data", label: "Data & Query" },
    ],
  },
  {
    label: "Analyze",
    items: [
      { href: "/insights", label: "Insights" },
      { href: "/explore", label: "Ask a Question" },
    ],
  },
  {
    label: "Configure",
    items: [{ href: "/settings", label: "Settings" }],
  },
];

const MATURITY_PCT: Record<string, number> = {
  raw: 20,
  profiled: 40,
  documented: 60,
  modeled: 80,
  production: 100,
};

export function AppSidebar() {
  const pathname = usePathname();
  const [progressState, setProgressState] = useState<{
    projectId: string;
    progress: ProjectProgress;
  } | null>(null);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const { activeProject } = useProjects();
  const [briefingCounts, setBriefingCounts] = useState<{
    high: number;
    total: number;
  }>({ high: 0, total: 0 });

  useEffect(() => {
    if (activeProject) {
      api
        .projectProgress(activeProject.id)
        .then((r) =>
          setProgressState({
            projectId: activeProject.id,
            progress: r.progress,
          })
        )
        .catch(() => {});
    }
    api.status().then(setStatus).catch(() => {});
    fetch("/api/briefing/today")
      .then((r) => (r.ok ? r.json() : null))
      .then((b) => {
        if (b) {
          setBriefingCounts({
            high: b.summary.attention_count,
            total: b.priorities.length,
          });
        }
      })
      .catch(() => {});
  }, [activeProject, pathname]);

  const martsPending = status
    ? Math.max(0, (status.mart_models ?? 0) - (status.executed ?? 0))
    : 0;
  const dictPending = status
    ? Math.max(0, (status.tables ?? 0) - (status.dictionary_reviewed ?? 0))
    : 0;
  const contractsCount = status?.contracts ?? 0;
  const maturityPct = activeProject
    ? MATURITY_PCT[activeProject.maturity ?? "raw"] ?? 20
    : 0;
  const progress =
    activeProject && progressState?.projectId === activeProject.id
      ? progressState.progress
      : null;

  return (
    <aside className="w-56 shrink-0 border-r border-border bg-card flex flex-col overflow-hidden">
      <div className="px-3 py-3 border-b border-border">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
          Review Queue
        </div>
        <QueueRow
          href="/models"
          label="Mart models"
          sub={martsPending ? "awaiting approval" : "all approved"}
          count={martsPending}
          tone="danger"
          active={pathname === "/models"}
        />
        <QueueRow
          href="/discovery"
          label="Column descriptions"
          sub={dictPending ? "need confirmation" : "all confirmed"}
          count={dictPending}
          tone="warning"
          active={pathname === "/discovery"}
        />
        <QueueRow
          href="/quality"
          label="Quality contracts"
          sub={`${contractsCount} tracked`}
          count={0}
          tone="success"
          active={pathname === "/quality"}
        />
        {briefingCounts.high > 0 && (
          <QueueRow
            href="/"
            label="Briefing priorities"
            sub={`${briefingCounts.high} high · ${briefingCounts.total} total`}
            count={briefingCounts.high}
            tone="danger"
            active={pathname === "/"}
          />
        )}
      </div>

      <nav className="px-2 py-3 flex-1 overflow-y-auto">
        {NAV_GROUPS.map((group) => (
          <div key={group.label} className="mb-3 last:mb-0">
            <div className="text-[10px] font-bold uppercase tracking-wider text-muted px-2 mb-1.5">
              {group.label}
            </div>
            {group.items.map((item) => {
              const active = pathname === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`block w-full text-left px-2.5 py-1.5 rounded-md text-sm transition-colors ${
                    active
                      ? "bg-accent/10 text-accent font-medium"
                      : "text-muted hover:text-foreground hover:bg-background/60"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
          </div>
        ))}
      </nav>

      {activeProject ? (
        <div className="m-3 p-3 bg-background border border-border rounded-lg">
          <div className="flex items-baseline justify-between mb-1.5">
            <span className="text-xs font-semibold text-foreground truncate">
              {activeProject.display_name}
            </span>
            <span className="text-[9px] uppercase tracking-wider text-muted bg-card border border-border rounded px-1.5 py-0.5">
              {activeProject.maturity ?? "raw"}
            </span>
          </div>
          <div className="h-1 bg-border rounded-full overflow-hidden mb-1">
            <div
              className="h-full bg-accent rounded-full transition-all"
              style={{ width: `${maturityPct}%` }}
            />
          </div>
          <div className="text-[10px] text-muted">
            {maturityPct}% maturity
            {progress &&
              ` · ${progress.tables_reviewed}/${progress.tables_discovered} tables reviewed`}
          </div>
        </div>
      ) : (
        <Link
          href="/projects"
          className="m-3 p-3 bg-background border border-border rounded-lg text-xs text-muted hover:text-foreground"
        >
          Create or select a project
        </Link>
      )}
    </aside>
  );
}

function QueueRow({
  href,
  label,
  sub,
  count,
  tone,
  active,
}: {
  href: string;
  label: string;
  sub: string;
  count: number;
  tone: "danger" | "warning" | "success";
  active: boolean;
}) {
  const TONE: Record<string, string> = {
    danger: "bg-red-50 text-red-600 dark:bg-red-900/30 dark:text-red-400",
    warning:
      "bg-amber-50 text-amber-600 dark:bg-amber-900/30 dark:text-amber-400",
    success:
      "bg-green-50 text-green-600 dark:bg-green-900/30 dark:text-green-400",
  };
  return (
    <Link
      href={href}
      className={`flex items-center justify-between w-full px-2 py-1.5 rounded-md mb-0.5 transition-colors ${
        active ? "bg-accent/10" : "hover:bg-background/60"
      }`}
    >
      <div className="min-w-0 pr-2">
        <div
          className={`text-[12px] font-medium truncate ${
            active ? "text-accent" : "text-foreground"
          }`}
        >
          {label}
        </div>
        <div className="text-[10px] text-muted truncate">{sub}</div>
      </div>
      {count > 0 ? (
        <span
          className={`shrink-0 text-[10px] font-bold px-2 py-0.5 rounded-full ${TONE[tone]}`}
        >
          {count}
        </span>
      ) : (
        <span className="shrink-0 text-[14px] text-green-600 dark:text-green-400">
          ✓
        </span>
      )}
    </Link>
  );
}
