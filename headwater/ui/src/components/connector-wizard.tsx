"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  type ConnectorType,
  type SourceCreatePayload,
} from "@/lib/api";
import { useToast } from "@/components/toast";

type Props = {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
};

export function ConnectorWizard({ open, onClose, onCreated }: Props) {
  const { toast } = useToast();
  const [step, setStep] = useState(1);
  const [chosen, setChosen] = useState<ConnectorType | null>(null);
  const [search, setSearch] = useState("");
  const [connectors, setConnectors] = useState<ConnectorType[]>([]);
  const [submitting, setSubmitting] = useState(false);

  const [form, setForm] = useState<SourceCreatePayload>({
    name: "my-new-source",
    type: "postgres",
    display_name: "",
    host: "",
    uri: "",
    path: "",
    auto_sync: true,
  });

  useEffect(() => {
    if (!open) return;
    api
      .connectorCatalog()
      .then((r) => setConnectors(r.connectors))
      .catch(() => {});
  }, [open]);

  useEffect(() => {
    if (!open) {
      setStep(1);
      setChosen(null);
      setSearch("");
    }
  }, [open]);

  const grouped = useMemo(() => {
    const filtered = connectors.filter((c) =>
      c.name.toLowerCase().includes(search.toLowerCase())
    );
    return filtered.reduce<Record<string, ConnectorType[]>>((acc, c) => {
      (acc[c.category] ||= []).push(c);
      return acc;
    }, {});
  }, [connectors, search]);

  if (!open) return null;

  const submit = async () => {
    if (!chosen) return;
    setSubmitting(true);
    try {
      await api.createSource({
        name: form.name,
        type: chosen.id,
        display_name: form.display_name || form.name,
        host: form.host || undefined,
        uri: form.uri || undefined,
        path: form.path || undefined,
        auto_sync: form.auto_sync,
      });
      toast(`Connected ${chosen.name}`, "success");
      onCreated();
      onClose();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(`Failed: ${msg}`, "error");
    }
    setSubmitting(false);
  };

  return (
    <div
      className="fixed inset-0 bg-black/40 backdrop-blur-sm z-50 flex items-center justify-center p-5"
      onClick={onClose}
    >
      <div
        className="bg-card rounded-xl w-full max-w-2xl max-h-[85vh] overflow-hidden flex flex-col shadow-2xl border border-border"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="px-6 py-4 border-b border-border flex items-center justify-between">
          <div>
            <div className="text-[11px] font-bold text-muted uppercase tracking-wider">
              Step {step} of 2
            </div>
            <div className="text-base font-bold mt-0.5">
              {step === 1
                ? "Choose a connector"
                : `Configure ${chosen?.name ?? ""}`}
            </div>
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-md bg-background hover:bg-border text-muted text-base"
            aria-label="Close"
          >
            ×
          </button>
        </div>

        <div className="px-6 py-5 overflow-y-auto flex-1">
          {step === 1 && (
            <>
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search connectors…"
                className="w-full px-3 py-2 border border-border rounded-md bg-background text-sm mb-4"
              />
              {Object.entries(grouped).map(([cat, types]) => (
                <div key={cat} className="mb-4">
                  <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
                    {cat}
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    {types.map((t) => (
                      <button
                        key={t.id}
                        onClick={() => {
                          if (!t.supported) {
                            toast(
                              `${t.name} is ${t.status ?? "planned"} and not supported in this build.`,
                              "info"
                            );
                            return;
                          }
                          setChosen(t);
                          setForm((f) => ({ ...f, type: t.id }));
                          setStep(2);
                        }}
                        className={`flex items-center gap-2.5 p-2.5 border rounded-lg text-left transition-colors ${
                          t.supported
                            ? "border-border hover:border-accent hover:bg-background"
                            : "border-border opacity-60 cursor-not-allowed"
                        }`}
                      >
                        <div
                          className="w-8 h-8 rounded-md flex items-center justify-center font-mono font-bold shrink-0"
                          style={{
                            background: t.color,
                            color: t.lightGlyph ? "#0f172a" : "#ffffff",
                            fontSize: t.glyph.length > 1 ? 10 : 13,
                          }}
                        >
                          {t.glyph}
                        </div>
                        <div className="min-w-0">
                          <div className="text-[13px] font-medium truncate">
                            {t.name}
                          </div>
                          {!t.supported && (
                            <div className="text-[10px] text-muted">
                              {t.status ?? "planned"}
                            </div>
                          )}
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </>
          )}

          {step === 2 && chosen && (
            <div className="flex flex-col gap-3.5">
              <Field
                label="Connection name"
                hint="Display name in Headwater"
                value={form.display_name ?? ""}
                onChange={(v) => setForm((f) => ({ ...f, display_name: v }))}
              />
              <Field
                label="Internal id (no spaces)"
                value={form.name}
                onChange={(v) => setForm((f) => ({ ...f, name: v }))}
              />
              {chosen.id === "postgres" || chosen.id === "mysql" ? (
                <Field
                  label="Connection URI"
                  hint="postgresql://user:pass@host:port/db"
                  value={form.uri ?? ""}
                  onChange={(v) => setForm((f) => ({ ...f, uri: v }))}
                />
              ) : null}
              {chosen.id === "json" || chosen.id === "csv" || chosen.id === "duckdb" ? (
                <Field
                  label="Path"
                  hint={
                    chosen.id === "duckdb"
                      ? "Absolute path to a .duckdb database file"
                      : "Absolute path to a directory of files"
                  }
                  value={form.path ?? ""}
                  onChange={(v) => setForm((f) => ({ ...f, path: v }))}
                />
              ) : null}
              <Field
                label="Host (optional)"
                value={form.host ?? ""}
                onChange={(v) => setForm((f) => ({ ...f, host: v }))}
              />
              <label className="flex items-center gap-2 text-[13px] text-foreground mt-2">
                <input
                  type="checkbox"
                  checked={form.auto_sync ?? false}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, auto_sync: e.target.checked }))
                  }
                />
                Auto-sync periodically
              </label>
            </div>
          )}
        </div>

        <div className="px-6 py-4 border-t border-border flex justify-between gap-2">
          <button
            onClick={step === 2 ? () => setStep(1) : onClose}
            className="px-3 py-1.5 border border-border rounded-md text-sm text-muted hover:bg-background"
          >
            {step === 2 ? "← Back" : "Cancel"}
          </button>
          {step === 2 && (
            <button
              onClick={submit}
              disabled={submitting}
              className="px-4 py-1.5 bg-accent text-white rounded-md text-sm font-medium hover:opacity-90 disabled:opacity-50"
            >
              {submitting ? "Connecting…" : "Test & Connect"}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

function Field({
  label,
  hint,
  value,
  onChange,
}: {
  label: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <div>
      <div className="flex gap-2 items-baseline mb-1.5">
        <label className="text-[13px] font-medium text-foreground">
          {label}
        </label>
        {hint && <span className="text-[11px] text-muted">{hint}</span>}
      </div>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full px-3 py-2 border border-border rounded-md bg-background text-sm font-mono"
      />
    </div>
  );
}
