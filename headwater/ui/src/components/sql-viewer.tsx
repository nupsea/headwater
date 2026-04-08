interface SqlViewerProps {
  sql: string;
}

export function SqlViewer({ sql }: SqlViewerProps) {
  return (
    <div className="bg-background border border-border rounded-lg overflow-auto">
      <pre className="p-4 text-sm font-mono leading-relaxed whitespace-pre-wrap">
        {sql}
      </pre>
    </div>
  );
}
