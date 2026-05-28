"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { h2, type H2Question, type H2ProposedQuestion, type H2RelevantColumn, type H2EdaFinding } from "@/lib/h2api";

const ANSWERABILITY_LABEL: Record<string, string> = {
  answerable: "Can answer",
  answerable_with_caveat: "With caveats",
  cannot_answer: "Cannot answer",
};

const ANSWERABILITY_COLOR: Record<string, string> = {
  answerable: "text-green-700 bg-green-50",
  answerable_with_caveat: "text-yellow-700 bg-yellow-50",
  cannot_answer: "text-red-700 bg-red-50",
};

export default function UnderstandPage() {
  const { id } = useParams<{ id: string }>();
  const [questions, setQuestions] = useState<H2Question[]>([]);
  const [relevance, setRelevance] = useState<H2RelevantColumn[]>([]);
  const [eda, setEda] = useState<H2EdaFinding[]>([]);
  const [edaScore, setEdaScore] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);

  const load = async () => {
    try {
      const project = await h2.projects.get(id);
      setQuestions(project.questions ?? []);
      const rel = await h2.projects.rerunRelevance(id);
      setRelevance(rel.relevant_columns);
    } finally {
      setLoading(false);
    }
  };

  const runEda = async () => {
    setRunning(true);
    try {
      const result = await h2.projects.eda.run(id);
      setEda(result.top_findings);
      setEdaScore(result.insight_confidence_score);
    } finally {
      setRunning(false);
    }
  };

  useEffect(() => { load(); }, [id]);

  if (loading) return <div className="p-8 text-gray-500">Loading…</div>;

  return (
    <div className="max-w-3xl mx-auto p-8">
      <div className="flex items-center gap-3 mb-6">
        <Link href={`/h2/projects/${id}`} className="text-xs text-gray-400 hover:text-gray-600">
          ← Project
        </Link>
        <h1 className="text-xl font-semibold text-gray-900">Understand</h1>
      </div>

      {/* Proposed questions */}
      <section className="mb-8">
        <h2 className="text-sm font-medium text-gray-700 mb-3">
          Proposed questions ({questions.length})
        </h2>
        {questions.length === 0 ? (
          <p className="text-sm text-gray-400">No questions yet. Re-frame the project to generate them.</p>
        ) : (
          <div className="space-y-3">
            {questions.map(q => (
              <QuestionRow key={q.id} question={q} />
            ))}
          </div>
        )}
      </section>

      {/* Relevant columns */}
      {relevance.length > 0 && (
        <section className="mb-8">
          <h2 className="text-sm font-medium text-gray-700 mb-3">
            Relevant columns (top {relevance.length})
          </h2>
          <div className="border border-gray-200 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-xs text-gray-500 uppercase">
                <tr>
                  <th className="px-4 py-2 text-left">Column</th>
                  <th className="px-4 py-2 text-left">Role</th>
                  <th className="px-4 py-2 text-right">Score</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {relevance.map(c => (
                  <tr key={`${c.table_name}.${c.column_name}`} className="hover:bg-gray-50">
                    <td className="px-4 py-2 font-mono text-xs">
                      <span className="text-gray-400">{c.table_name}.</span>
                      {c.column_name}
                    </td>
                    <td className="px-4 py-2 text-gray-500 text-xs">{c.semantic_role ?? "—"}</td>
                    <td className="px-4 py-2 text-right">
                      <span className="text-xs font-medium text-gray-700">{c.score.toFixed(1)}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* EDA */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-medium text-gray-700">
            Data quality findings
            {edaScore !== null && (
              <span className="ml-2 text-xs text-gray-400">
                insight confidence {(edaScore * 100).toFixed(0)}%
              </span>
            )}
          </h2>
          <button
            onClick={runEda}
            disabled={running}
            className="text-xs px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50"
          >
            {running ? "Running…" : "Run EDA"}
          </button>
        </div>
        {eda.length > 0 ? (
          <div className="space-y-2">
            {eda.map((f, i) => (
              <EdaFindingRow key={i} finding={f} />
            ))}
          </div>
        ) : (
          <p className="text-sm text-gray-400">
            Click "Run EDA" to analyse data quality patterns.
          </p>
        )}
      </section>

      <div className="mt-8 flex justify-end">
        <Link
          href={`/h2/projects/${id}/resolve`}
          className="px-5 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          Resolve →
        </Link>
      </div>
    </div>
  );
}

function QuestionRow({ question }: { question: H2Question }) {
  const answerability = question.answerability;
  return (
    <div className="border border-gray-200 rounded-lg p-4">
      <div className="flex items-start gap-3">
        <span className={`text-xs font-medium px-2 py-0.5 rounded shrink-0 mt-0.5 ${ANSWERABILITY_COLOR[answerability] ?? "text-gray-600 bg-gray-50"}`}>
          {ANSWERABILITY_LABEL[answerability] ?? answerability}
        </span>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-gray-800">{question.title}</p>
          {question.question.reason && (
            <p className="text-xs text-gray-500 mt-0.5">{question.question.reason}</p>
          )}
          {question.question.needed_columns && question.question.needed_columns.length > 0 && (
            <div className="mt-1 flex flex-wrap gap-1">
              {question.question.needed_columns.map(c => (
                <span key={c} className="text-xs font-mono bg-gray-100 text-gray-600 px-1.5 py-0.5 rounded">
                  {c}
                </span>
              ))}
            </div>
          )}
        </div>
        <span className="text-xs text-gray-400 shrink-0">
          {(question.confidence * 100).toFixed(0)}%
        </span>
      </div>
    </div>
  );
}

function EdaFindingRow({ finding }: { finding: H2EdaFinding }) {
  const isCritical = finding.flags.includes("critical");
  return (
    <div className={`border rounded-lg px-4 py-3 text-sm ${isCritical ? "border-red-200 bg-red-50" : "border-gray-200"}`}>
      <div className="flex items-start gap-2">
        <span className="text-xs text-gray-400 shrink-0 mt-0.5 font-medium uppercase">{finding.family}</span>
        <div className="flex-1">
          <p className={`text-sm font-medium ${isCritical ? "text-red-800" : "text-gray-800"}`}>
            {finding.title}
          </p>
          <div className="flex gap-3 mt-1 text-xs text-gray-400">
            <span>effect {(finding.effect_size * 100).toFixed(0)}%</span>
            <span>conf {(finding.confidence * 100).toFixed(0)}%</span>
            {finding.flags.filter(f => f !== "critical").map(f => (
              <span key={f} className="bg-gray-100 px-1.5 rounded">{f}</span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
