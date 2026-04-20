"use client";

import { useState } from "react";

interface QuestionResolverProps {
  questions: string[];
  onAnswer: (answers: { question_index: number; answer: string }[]) => Promise<void>;
}

export function QuestionResolver({
  questions,
  onAnswer,
}: QuestionResolverProps) {
  const [answers, setAnswers] = useState<Record<number, string>>({});
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [error, setError] = useState("");

  if (questions.length === 0) return null;

  if (submitted) {
    return (
      <div className="bg-green-50 border border-green-200 rounded-lg p-4">
        <div className="text-sm text-green-800 font-medium">
          Answers submitted successfully.
        </div>
        <p className="text-xs text-green-700 mt-1">
          These answers will inform model refinement on the next pipeline run.
        </p>
      </div>
    );
  }

  const handleSubmit = async () => {
    const filled = Object.entries(answers)
      .filter(([, v]) => v.trim())
      .map(([idx, answer]) => ({
        question_index: Number(idx),
        answer: answer.trim(),
      }));
    if (filled.length === 0) {
      setError("Provide at least one answer before submitting.");
      return;
    }
    setSubmitting(true);
    setError("");
    try {
      await onAnswer(filled);
      setSubmitted(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSubmitting(false);
  };

  return (
    <div className="bg-warning/5 border border-warning/30 rounded-lg p-4">
      <h4 className="text-xs font-semibold text-warning uppercase tracking-wide mb-3">
        Questions for Review ({questions.length})
      </h4>

      {error && (
        <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700">
          {error}
        </div>
      )}

      <div className="space-y-3">
        {questions.map((q, i) => (
          <div key={i}>
            <div className="flex items-start gap-2 text-sm mb-1.5">
              <span className="text-warning font-bold mt-0.5">?</span>
              <span>{q}</span>
            </div>
            <textarea
              value={answers[i] || ""}
              onChange={(e) =>
                setAnswers((prev) => ({ ...prev, [i]: e.target.value }))
              }
              placeholder="Your answer..."
              rows={2}
              className="w-full px-3 py-1.5 border border-border rounded bg-background text-sm resize-none"
            />
          </div>
        ))}
      </div>

      <div className="mt-3 flex justify-end">
        <button
          onClick={handleSubmit}
          disabled={submitting}
          className="px-4 py-1.5 bg-accent text-white rounded text-xs font-medium hover:opacity-90 disabled:opacity-50 transition-opacity"
        >
          {submitting ? "Submitting..." : "Submit Answers"}
        </button>
      </div>
    </div>
  );
}
