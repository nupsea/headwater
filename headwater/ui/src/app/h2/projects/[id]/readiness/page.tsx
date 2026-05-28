"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { h2, type H2ReadinessReport, type H2QuestionReadiness, stateColor } from "@/lib/h2api";

export default function ReadinessPage() {
  const { id } = useParams<{ id: string }>();
  const [report, setReport] = useState<H2ReadinessReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [evaluating, setEvaluating] = useState(false);
  const [certifying, setCertifying] = useState(false);
  const [demotions, setDemotions] = useState<string[]>([]);

  const evaluate = async () => {
    setEvaluating(true);
    try {
      const r = await h2.projects.readiness.evaluate(id);
      setReport(r);
    } finally {
      setEvaluating(false);
    }
  };

  const certify = async () => {
    setCertifying(true);
    try {
      const result = await h2.projects.certify.check(id);
      setDemotions(result.demotions.map((d: { question_title: string }) => d.question_title));
      await evaluate();
    } finally {
      setCertifying(false);
    }
  };

  useEffect(() => {
    h2.projects.readiness.evaluate(id).then(setReport).finally(() => setLoading(false));
  }, [id]);

  if (loading) return <div className="p-8 text-gray-500">Loading…</div>;

  return (
    <div className="max-w-3xl mx-auto p-8">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Link href={`/h2/projects/${id}`} className="text-xs text-gray-400 hover:text-gray-600">
            ← Project
          </Link>
          <h1 className="text-xl font-semibold text-gray-900">Readiness</h1>
        </div>
        <div className="flex gap-2">
          <button
            onClick={evaluate}
            disabled={evaluating}
            className="text-xs px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50"
          >
            {evaluating ? "Evaluating…" : "Re-evaluate"}
          </button>
          <button
            onClick={certify}
            disabled={certifying}
            className="text-xs px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
          >
            {certifying ? "Checking…" : "Certify check"}
          </button>
        </div>
      </div>

      {demotions.length > 0 && (
        <div className="border border-orange-200 bg-orange-50 rounded-lg px-4 py-3 mb-6">
          <p className="text-sm font-medium text-orange-800">
            {demotions.length} question{demotions.length !== 1 ? "s" : ""} demoted
          </p>
          <ul className="mt-1 text-xs text-orange-700 list-disc list-inside">
            {demotions.map(d => <li key={d}>{d}</li>)}
          </ul>
        </div>
      )}

      {report && (
        <>
          {/* Summary */}
          <div className="grid grid-cols-3 gap-4 mb-6">
            <SummaryCell label="Certified" count={report.certified_count} color="text-green-600" />
            <SummaryCell label="Draft" count={report.draft_count} color="text-yellow-600" />
            <SummaryCell label="Cannot answer" count={report.cannot_answer_count} color="text-red-500" />
          </div>

          {/* Per-question verdicts */}
          <div className="space-y-3">
            {report.questions.map(q => (
              <QuestionVerdictRow key={q.question_id} question={q} />
            ))}
          </div>
        </>
      )}

      <div className="mt-8 flex justify-end">
        <Link
          href={`/h2/projects/${id}/answer`}
          className="px-5 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          Answer →
        </Link>
      </div>
    </div>
  );
}

function SummaryCell({ label, count, color }: { label: string; count: number; color: string }) {
  return (
    <div className="border border-gray-200 rounded-lg p-4 text-center">
      <div className={`text-2xl font-semibold ${color}`}>{count}</div>
      <div className="text-xs text-gray-500 mt-1">{label}</div>
    </div>
  );
}

function QuestionVerdictRow({ question }: { question: H2QuestionReadiness }) {
  const [open, setOpen] = useState(false);
  const shortId = question.question_id.split(":").pop() ?? question.question_id;

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50"
      >
        <span className={`text-xs font-medium px-2 py-0.5 rounded border shrink-0 ${stateColor(question.state)}`}>
          {question.state.replace("_", " ").toUpperCase()}
        </span>
        <div className="flex-1 min-w-0">
          <p className="text-sm text-gray-800 font-medium truncate">{shortId}</p>
          {question.summary && (
            <p className="text-xs text-gray-500 truncate">{question.summary}</p>
          )}
        </div>
        <div className="text-right shrink-0">
          <div className="text-sm font-medium text-gray-700">{question.readiness_pct}%</div>
          <div className="text-xs text-gray-400">readiness</div>
        </div>
        <span className="text-gray-400 text-xs">{open ? "▲" : "▼"}</span>
      </button>

      {open && question.contracts.length > 0 && (
        <div className="border-t border-gray-100 px-4 py-3 bg-gray-50">
          <div className="space-y-1.5">
            {question.contracts.map(c => (
              <div key={c.contract_type} className="flex items-start gap-2 text-xs">
                <span className={c.passed ? "text-green-600" : "text-red-600"}>
                  {c.passed ? "✓" : "✗"}
                </span>
                <span className="font-medium text-gray-600 shrink-0">{c.contract_type}:</span>
                <span className="text-gray-500">{c.note}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
