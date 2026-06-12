"use client";

import { useCallback, useEffect, useState } from "react";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

export interface ConfirmOptions {
  title: string;
  body: string;
  confirmLabel?: string;
  cancelLabel?: string;
  /** Destructive actions get a red confirm button. */
  danger?: boolean;
}

function ConfirmDialog({
  opts,
  onClose,
}: {
  opts: ConfirmOptions;
  onClose: (confirmed: boolean) => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose(false);
      if (e.key === "Enter") onClose(true);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label={opts.title}
      onClick={() => onClose(false)}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(20,20,25,0.32)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 20,
        zIndex: 60,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: HW2_COLOR.paper,
          border: `1px solid ${HW2_COLOR.rule2}`,
          borderRadius: 14,
          width: "100%",
          maxWidth: 440,
          padding: "22px 24px 18px",
          fontFamily: "'DM Sans', sans-serif",
          boxShadow: "0 12px 40px rgba(20,20,30,0.18)",
        }}
      >
        <h2
          style={{
            font: "600 16px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink,
            letterSpacing: "-0.01em",
            margin: "0 0 8px",
          }}
        >
          {opts.title}
        </h2>
        <p
          style={{
            font: "400 13.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
            lineHeight: 1.55,
            margin: "0 0 18px",
            whiteSpace: "pre-line",
          }}
        >
          {opts.body}
        </p>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
          <button
            onClick={() => onClose(false)}
            autoFocus
            style={{
              appearance: "none",
              cursor: "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "9px 16px",
              font: "500 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            {opts.cancelLabel ?? "Cancel"}
          </button>
          <button
            onClick={() => onClose(true)}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: opts.danger ? HW2_COLOR.bad : HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "9px 18px",
              font: "600 13px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            {opts.confirmLabel ?? "Confirm"}
          </button>
        </div>
      </div>
    </div>
  );
}

/** Promise-based styled replacement for window.confirm.
 *
 *  const { confirm, confirmDialog } = useConfirm();
 *  ...render {confirmDialog} once...
 *  if (!(await confirm({ title, body, danger: true }))) return;
 */
export function useConfirm() {
  const [pending, setPending] = useState<{
    opts: ConfirmOptions;
    resolve: (v: boolean) => void;
  } | null>(null);

  const confirm = useCallback(
    (opts: ConfirmOptions) =>
      new Promise<boolean>((resolve) => setPending({ opts, resolve })),
    []
  );

  const close = useCallback(
    (confirmed: boolean) => {
      pending?.resolve(confirmed);
      setPending(null);
    },
    [pending]
  );

  const confirmDialog = pending ? (
    <ConfirmDialog opts={pending.opts} onClose={close} />
  ) : null;

  return { confirm, confirmDialog };
}
