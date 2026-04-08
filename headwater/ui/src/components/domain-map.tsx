interface Props {
  domains: Record<string, { tables: string[]; total_rows: number }>;
}

const domainColors: Record<string, string> = {
  "Environmental Monitoring": "border-green-500/40 bg-green-500/5",
  "Public Health": "border-red-500/40 bg-red-500/5",
  "Facility & Inspection": "border-blue-500/40 bg-blue-500/5",
  "Community Engagement": "border-purple-500/40 bg-purple-500/5",
  "Programs & Interventions": "border-orange-500/40 bg-orange-500/5",
  "Geography & Demographics": "border-teal-500/40 bg-teal-500/5",
};

export function DomainMap({ domains }: Props) {
  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-4">
        Domain Classification
      </h3>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {Object.entries(domains).map(([domain, info]) => (
          <div
            key={domain}
            className={`border rounded-lg p-3 ${domainColors[domain] || "border-border"}`}
          >
            <div className="text-sm font-semibold">{domain}</div>
            <div className="text-xs text-muted mt-1">
              {info.total_rows.toLocaleString()} total rows
            </div>
            <div className="flex flex-wrap gap-1 mt-2">
              {info.tables.map((t) => (
                <span
                  key={t}
                  className="px-2 py-0.5 bg-background rounded text-xs font-mono"
                >
                  {t}
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
