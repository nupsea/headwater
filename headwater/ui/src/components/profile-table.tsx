import type { ColumnProfile } from "@/lib/api";

interface ProfileTableProps {
  profiles: ColumnProfile[];
}

export function ProfileTable({ profiles }: ProfileTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-left text-muted">
            <th className="py-2 pr-4">Column</th>
            <th className="py-2 pr-4">Type</th>
            <th className="py-2 pr-4 text-right">Nulls</th>
            <th className="py-2 pr-4 text-right">Distinct</th>
            <th className="py-2 pr-4 text-right">Uniqueness</th>
            <th className="py-2 pr-4">Range / Top Values</th>
            <th className="py-2 pr-4">Pattern</th>
          </tr>
        </thead>
        <tbody>
          {profiles.map((p) => (
            <tr key={p.column_name} className="border-b border-border/50">
              <td className="py-2 pr-4 font-mono font-medium">
                {p.column_name}
              </td>
              <td className="py-2 pr-4 text-muted">{p.dtype}</td>
              <td className="py-2 pr-4 text-right">
                {(p.null_rate * 100).toFixed(1)}%
              </td>
              <td className="py-2 pr-4 text-right">
                {p.distinct_count.toLocaleString()}
              </td>
              <td className="py-2 pr-4 text-right">
                {(p.uniqueness_ratio * 100).toFixed(1)}%
              </td>
              <td className="py-2 pr-4 text-xs">
                {p.min_value !== null && p.max_value !== null ? (
                  <span>
                    [{p.min_value}, {p.max_value}]
                  </span>
                ) : p.top_values && p.top_values.length > 0 ? (
                  <span className="text-muted">
                    {p.top_values
                      .slice(0, 3)
                      .map(([v]) => v)
                      .join(", ")}
                    {p.top_values.length > 3 && "..."}
                  </span>
                ) : (
                  "-"
                )}
              </td>
              <td className="py-2 pr-4">
                {p.detected_pattern ? (
                  <span className="px-1.5 py-0.5 bg-accent/10 text-accent rounded text-xs">
                    {p.detected_pattern}
                  </span>
                ) : (
                  "-"
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
