"use client";

import { useState } from "react";
import type { ActivityEntry } from "@/lib/api";

function relativeTime(isoDate: string): string {
  const now = Date.now();
  const then = new Date(isoDate).getTime();
  const diffMs = now - then;
  const diffSec = Math.floor(diffMs / 1000);

  if (diffSec < 60) return "just now";
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  if (diffDay < 7) return `${diffDay}d ago`;
  return new Date(isoDate).toLocaleDateString();
}

const VISIBLE_DEFAULT = 5;

export function ActivityFeed({
  activities,
}: {
  activities: ActivityEntry[];
}) {
  const [showAll, setShowAll] = useState(false);

  const visible = showAll ? activities : activities.slice(0, VISIBLE_DEFAULT);
  const hasMore = activities.length > VISIBLE_DEFAULT;

  if (activities.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-5">
        <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
          Recent Activity
        </h3>
        <p className="text-sm text-muted">No activity recorded yet.</p>
      </div>
    );
  }

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
        Recent Activity
      </h3>
      <div className="relative">
        {/* Vertical timeline line */}
        <div className="absolute left-[5px] top-2 bottom-2 w-px bg-border" />

        <div className="space-y-3">
          {visible.map((entry) => (
            <div key={entry.id} className="flex items-start gap-3 relative">
              {/* Timeline dot */}
              <div className="w-[11px] h-[11px] rounded-full bg-border border-2 border-card shrink-0 mt-1 relative z-10" />

              <div className="min-w-0 flex-1">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-xs font-medium truncate">
                    {entry.action}
                  </span>
                  <span className="text-[10px] text-muted shrink-0">
                    {relativeTime(entry.created_at)}
                  </span>
                </div>
                {entry.detail && (
                  <div className="text-[10px] text-muted mt-0.5 truncate">
                    {entry.detail}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {hasMore && (
        <button
          onClick={() => setShowAll(!showAll)}
          className="mt-3 text-xs text-accent hover:underline"
        >
          {showAll
            ? "Show less"
            : `Show ${activities.length - VISIBLE_DEFAULT} more`}
        </button>
      )}
    </div>
  );
}
