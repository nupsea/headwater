"use client";

import dynamic from "next/dynamic";

function ExploreLoading() {
  return (
    <div>
      <div className="mb-6 flex flex-col gap-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold mb-2">Explore Data</h1>
            <p className="text-muted text-sm max-w-2xl">
              Ask natural language questions about your data. The system decomposes
              questions into metrics and dimensions from the semantic catalog, then
              generates deterministic SQL.
            </p>
          </div>
        </div>
      </div>
      <div className="mb-6">
        <div className="flex gap-2">
          <input
            type="text"
            value=""
            readOnly
            placeholder="Ask a question about your data..."
            className="flex-1 px-4 py-2 border border-border rounded-lg bg-background text-sm"
          />
          <button
            disabled
            className="px-4 py-2 bg-foreground text-background rounded-lg text-sm font-medium disabled:opacity-50"
          >
            Ask
          </button>
        </div>
      </div>
    </div>
  );
}

const ExploreClientPage = dynamic(() => import("./explore-client"), {
  ssr: false,
  loading: () => <ExploreLoading />,
});

export default function ExplorePageClientOnly() {
  return <ExploreClientPage />;
}
