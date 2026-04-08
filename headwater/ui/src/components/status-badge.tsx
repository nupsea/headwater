interface StatusBadgeProps {
  status: string;
}

const statusStyles: Record<string, string> = {
  approved: "bg-success/15 text-success",
  proposed: "bg-warning/15 text-warning",
  rejected: "bg-danger/15 text-danger",
  executed: "bg-accent/15 text-accent",
  observing: "bg-accent/15 text-accent",
  enforced: "bg-success/15 text-success",
  disabled: "bg-muted/15 text-muted",
};

export function StatusBadge({ status }: StatusBadgeProps) {
  const style = statusStyles[status] || "bg-muted/15 text-muted";
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${style}`}>
      {status}
    </span>
  );
}
