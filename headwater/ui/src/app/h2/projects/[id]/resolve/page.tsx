"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { h2, type H2ResolveCard, priorityColor } from "@/lib/h2api";

export default function ResolvePage() {
  const { id } = useParams<{ id: string }>();
  const [cards, setCards] = useState<H2ResolveCard[]>([]);
  const [loading, setLoading] = useState(true);
  const [rebuilding, setRebuilding] = useState(false);

  const load = () =>
    h2.projects.resolve.list(id)
      .then(c => setCards(c as H2ResolveCard[]))
      .finally(() => setLoading(false));

  const rebuild = async () => {
    setRebuilding(true);
    try {
      const result = await h2.projects.resolve.build(id);
      setCards(result);
    } finally {
      setRebuilding(false);
    }
  };

  useEffect(() => { load(); }, [id]);

  const byPriority = {
    high: cards.filter(c => c.priority === "high"),
    medium: cards.filter(c => c.priority === "medium"),
    low: cards.filter(c => c.priority === "low"),
  };

  if (loading) return <div className="p-8 text-gray-500">Loading…</div>;

  return (
    <div className="max-w-3xl mx-auto p-8">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <Link href={`/h2/projects/${id}`} className="text-xs text-gray-400 hover:text-gray-600">
            ← Project
          </Link>
          <h1 className="text-xl font-semibold text-gray-900">
            Resolve
            {cards.length > 0 && (
              <span className="ml-2 text-sm font-normal text-gray-400">
                {cards.length} item{cards.length !== 1 ? "s" : ""}
              </span>
            )}
          </h1>
        </div>
        <button
          onClick={rebuild}
          disabled={rebuilding}
          className="text-xs px-3 py-1 border border-gray-300 rounded hover:bg-gray-50 disabled:opacity-50"
        >
          {rebuilding ? "Rebuilding…" : "Rebuild"}
        </button>
      </div>

      <p className="text-sm text-gray-500 mb-6">
        These are the decisions that have the most impact on your project's readiness verdict.
        Resolving them unlocks certification for affected questions.
      </p>

      {cards.length === 0 ? (
        <div className="border border-dashed border-gray-300 rounded-lg p-8 text-center text-gray-500">
          <p>No resolve items.</p>
          <p className="text-sm mt-1">Click "Rebuild" to analyse the project for gaps.</p>
        </div>
      ) : (
        <div className="space-y-6">
          {(["high", "medium", "low"] as const).map(priority => {
            const group = byPriority[priority];
            if (group.length === 0) return null;
            return (
              <div key={priority}>
                <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-2">
                  {priority} priority
                </h2>
                <div className="space-y-3">
                  {group.map(card => (
                    <ResolveCardRow key={card.card_id} card={card} />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}

      <div className="mt-8 flex justify-end">
        <Link
          href={`/h2/projects/${id}/readiness`}
          className="px-5 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          Readiness →
        </Link>
      </div>
    </div>
  );
}

function ResolveCardRow({ card }: { card: H2ResolveCard }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-start gap-3 px-4 py-3 text-left hover:bg-gray-50"
      >
        <span className={`text-xs font-medium px-2 py-0.5 rounded shrink-0 mt-0.5 ${priorityColor(card.priority)}`}>
          {card.priority.toUpperCase()}
        </span>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-medium text-gray-800">{card.title}</p>
          <p className="text-xs text-gray-500 mt-0.5">{card.issue_kind}</p>
        </div>
        <span className="text-gray-400 text-xs shrink-0">{open ? "▲" : "▼"}</span>
      </button>

      {open && (
        <div className="border-t border-gray-100 px-4 py-3 bg-gray-50">
          <p className="text-sm text-gray-700">{card.body}</p>
          {card.affected_questions.length > 0 && (
            <p className="text-xs text-gray-500 mt-2">
              Affects {card.affected_questions.length} question(s).
              Clears contracts: {card.contract_impacts.join(", ")}.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
