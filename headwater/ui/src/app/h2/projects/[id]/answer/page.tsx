"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { h2, type H2AnswerDraft, stateColor } from "@/lib/h2api";

export default function AnswerPage() {
  const { id } = useParams<{ id: string }>();
  const [answers, setAnswers] = useState<H2AnswerDraft[]>([]);
  const [counts, setCounts] = useState({ certified: 0, draft: 0, cannot_answer: 0 });
  const [loading, setLoading] = useState(true);
  const [drafting, setDrafting] = useState(false);
  const [report, setReport] = useState<string | null>(null);
  const [loadingReport, setLoadingReport] = useState(false);

  const draft = async () => {
    setDrafting(true);
    try {
      const result = await h2.projects.answer.draft(id);
      setAnswers(result.answers);
      setCounts({
        certified: result.certified_count,
        draft: result.draft_count,
        cannot_answer: result.cannot_answer_count,
      });
    } finally {
      setDrafting(false);
    }
  };

  const fetchReport = async () => {
    setLoadingReport(true);
    try {
      const text = await h2.projects.report.get(id);
      setReport(text);
    } finally {
      setLoadingReport(false);
    }
  };

  useEffect(() => {
    h2.projects.answer.draft(id)
      .then(r => {
        setAnswers(r.answers);
        setCounts({ certified: r.certified_count, draft: r.draft_count, cannot_answer: r.cannot_answer_count });
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) return <div className="p-8 text-gray-500">Loading…</div>;

  return (
    <div className="max-w-3xl mx-auto p-8">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Link href={`/h2/projects/${id}`} className="text-xs text-gray-400 hover:text-gray-600">
            ← Project
          </Link>
          <h1 className="text-xl font-semibold text-gray-900">Answer & Share</h1>
        </div>
        <div className="flex gap-2">
          <button
            onClick={draft}
            disabled={drafting}
            className="text-xs px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50"
          >
            {drafting ? "Drafting…" : "Redraft"}
          </button>
          <button
            onClick={fetchReport}
            disabled={loadingReport}
            className="text-xs px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
          >
            {loadingReport ? "Loading…" : "Export report"}
          </button>
        </div>
      </div>

      {/* Summary */}
      <div className="flex gap-4 mb-6 text-sm">
        <span className="text-green-700">{counts.certified} certified</span>
        <span className="text-gray-400">·</span>
        <span className="text-yellow-700">{counts.draft} draft</span>
        <span className="text-gray-400">·</span>
        <span className="text-red-600">{counts.cannot_answer} cannot answer</span>
      </div>

      {/* Answer cards */}
      <div className="space-y-4">
        {answers.map(a => (
          <AnswerCard key={a.question_id} answer={a} />
        ))}
      </div>

      {/* Markdown report */}
      {report && (
        <div className="mt-8 border border-gray-200 rounded-lg overflow-hidden">
          <div className="flex items-center justify-between bg-gray-50 px-4 py-2">
            <span className="text-sm font-medium text-gray-700">Audit report (Markdown)</span>
            <button
              onClick={() => {
                const blob = new Blob([report], { type: "text/markdown" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = `${id}_report.md`;
                a.click();
                URL.revokeObjectURL(url);
              }}
              className="text-xs text-blue-600 hover:underline"
            >
              Download
            </button>
          </div>
          <pre className="p-4 text-xs text-gray-700 overflow-auto max-h-96 bg-white whitespace-pre-wrap">
            {report}
          </pre>
        </div>
      )}
    </div>
  );
}

function AnswerCard({ answer }: { answer: H2AnswerDraft }) {
  const [open, setOpen] = useState(false);
  const shortId = answer.question_id.split(":").pop() ?? answer.question_id;

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50"
      >
        <span className={`text-xs font-medium px-2 py-0.5 rounded border shrink-0 ${stateColor(answer.state)}`}>
          {answer.state.replace("_", " ").toUpperCase()}
        </span>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-gray-800 truncate">{answer.question_title}</p>
          {answer.chart_spec?.type && (
            <span className="text-xs text-gray-400">
              {String(answer.chart_spec.type)} chart
            </span>
          )}
        </div>
        <div className="text-right shrink-0">
          <div className="text-sm font-medium text-gray-700">
            {(answer.confidence * 100).toFixed(0)}%
          </div>
        </div>
        {answer.sql_text && (
          <span className="text-gray-400 text-xs">{open ? "▲" : "▼"}</span>
        )}
      </button>

      {open && answer.sql_text && (
        <div className="border-t border-gray-100">
          {answer.caveats.length > 0 && (
            <div className="px-4 py-2 bg-yellow-50 border-b border-yellow-100">
              {answer.caveats.map((c, i) => (
                <p key={i} className="text-xs text-yellow-700">⚠ {c}</p>
              ))}
            </div>
          )}
          <pre className="px-4 py-3 text-xs text-gray-700 bg-gray-50 overflow-auto">
            {answer.sql_text}
          </pre>
        </div>
      )}

      {answer.state === "cannot_answer" && answer.caveats.length > 0 && (
        <div className="border-t border-gray-100 px-4 py-2 bg-red-50">
          <p className="text-xs text-red-700">{answer.caveats[0]}</p>
        </div>
      )}
    </div>
  );
}
