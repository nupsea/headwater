import Link from "next/link";

interface ReviewQueueProps {
  dictPending: number;
  modelsPending: number;
  contractsObserving: number;
}

interface ReviewGroup {
  label: string;
  count: number;
  link: string;
  description: string;
}

export function ReviewQueue({
  dictPending,
  modelsPending,
  contractsObserving,
}: ReviewQueueProps) {
  const groups: ReviewGroup[] = [
    {
      label: "Dictionary Review",
      count: dictPending,
      link: "/discovery",
      description: "Tables awaiting column and metadata review",
    },
    {
      label: "Model Review",
      count: modelsPending,
      link: "/models",
      description: "Mart models requiring human approval",
    },
    {
      label: "Quality Review",
      count: contractsObserving,
      link: "/quality",
      description: "Contracts in observation mode",
    },
  ];

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
        Review Queue
      </h3>
      <div className="space-y-2">
        {groups.map((group) => (
          <Link
            key={group.label}
            href={group.link}
            className="flex items-center justify-between p-3 rounded-lg border border-border hover:bg-background transition-colors group"
          >
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-sm font-medium">{group.label}</span>
                {group.count > 0 ? (
                  <span className="inline-flex items-center justify-center h-5 min-w-[20px] px-1.5 rounded-full bg-accent text-white text-[10px] font-bold">
                    {group.count}
                  </span>
                ) : (
                  <span className="text-xs text-success font-medium">
                    All reviewed
                  </span>
                )}
              </div>
              <div className="text-[10px] text-muted mt-0.5">
                {group.description}
              </div>
            </div>
            <span className="text-xs text-muted shrink-0 ml-2 opacity-0 group-hover:opacity-100 transition-opacity">
              View &rarr;
            </span>
          </Link>
        ))}
      </div>
    </div>
  );
}
