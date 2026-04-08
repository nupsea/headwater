"use client";

import React, { useMemo } from "react";

interface SqlViewerProps {
  sql: string;
}

// SQL keywords to highlight
const KEYWORDS = new Set([
  "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
  "FULL", "CROSS", "ON", "AND", "OR", "NOT", "IN", "IS", "NULL",
  "AS", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
  "ASC", "DESC", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END",
  "UNION", "ALL", "EXISTS", "BETWEEN", "LIKE", "WITH", "RECURSIVE",
  "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE",
  "INTO", "VALUES", "SET", "CAST", "OVER", "PARTITION",
]);

const FUNCTIONS = new Set([
  "COUNT", "SUM", "AVG", "MIN", "MAX", "ROUND", "COALESCE",
  "DATE_TRUNC", "EXTRACT", "CONCAT", "LENGTH", "UPPER", "LOWER",
  "TRIM", "REPLACE", "SUBSTRING", "ABS", "CEIL", "FLOOR",
  "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD",
  "FIRST_VALUE", "LAST_VALUE", "NTILE",
]);

/** Add line breaks before major SQL clauses for readability. */
function formatSql(raw: string): string {
  // Normalize whitespace
  let sql = raw.replace(/\s+/g, " ").trim();

  // Break before major clauses (but not when inside parentheses)
  const breakBefore = [
    "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
    "LIMIT", "OFFSET", "JOIN", "LEFT JOIN", "RIGHT JOIN",
    "INNER JOIN", "FULL JOIN", "CROSS JOIN", "LEFT OUTER JOIN",
    "ON", "AND", "OR", "UNION", "WITH", "WINDOW",
  ];

  // Sort longest first to match "LEFT JOIN" before "LEFT"
  const sorted = breakBefore.sort((a, b) => b.length - a.length);

  for (const kw of sorted) {
    // Case-insensitive replace, add newline before the keyword
    const regex = new RegExp(`(?<!^)\\s+(?=${kw.replace(/ /g, "\\s+")}\\b)`, "gi");
    sql = sql.replace(regex, "\n");
  }

  // Indent continuation lines (anything after the first line)
  const lines = sql.split("\n");
  const formatted = lines.map((line, i) => {
    if (i === 0) return line.trim();
    const trimmed = line.trim();
    const upper = trimmed.toUpperCase();
    // Major clauses at indent level 0
    if (/^(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|WITH|UNION)\b/i.test(trimmed)) {
      return trimmed;
    }
    // JOINs at indent level 1
    if (/^(JOIN|LEFT|RIGHT|INNER|FULL|CROSS)\b/i.test(trimmed)) {
      return "  " + trimmed;
    }
    // ON, AND, OR at indent level 2
    if (/^(ON|AND|OR)\b/i.test(upper)) {
      return "    " + trimmed;
    }
    return "  " + trimmed;
  });

  return formatted.join("\n");
}

/** Tokenize and highlight SQL. */
function highlightSql(sql: string): React.ReactElement[] {
  const elements: React.ReactElement[] = [];
  // Match: strings, numbers, comments, words, or other characters
  const tokenRegex = /('(?:[^'\\]|\\.)*')|("(?:[^"\\]|\\.)*")|(\b\d+(?:\.\d+)?\b)|(--[^\n]*)|(\b[A-Za-z_][A-Za-z0-9_]*\b)|(\n)|(.)/g;

  let match: RegExpExecArray | null;
  let idx = 0;

  while ((match = tokenRegex.exec(sql)) !== null) {
    const [full, singleStr, doubleStr, num, comment, word, newline, other] = match;

    if (singleStr) {
      // String literal
      elements.push(
        <span key={idx++} className="text-green-600 dark:text-green-400">{full}</span>
      );
    } else if (doubleStr) {
      // Quoted identifier
      elements.push(
        <span key={idx++} className="text-amber-600 dark:text-amber-400">{full}</span>
      );
    } else if (num) {
      // Number
      elements.push(
        <span key={idx++} className="text-purple-600 dark:text-purple-400">{full}</span>
      );
    } else if (comment) {
      elements.push(
        <span key={idx++} className="text-gray-400 italic">{full}</span>
      );
    } else if (word) {
      const upper = word.toUpperCase();
      if (KEYWORDS.has(upper)) {
        elements.push(
          <span key={idx++} className="text-blue-600 dark:text-blue-400 font-semibold">{word.toUpperCase()}</span>
        );
      } else if (FUNCTIONS.has(upper)) {
        elements.push(
          <span key={idx++} className="text-amber-600 dark:text-amber-400">{word.toUpperCase()}</span>
        );
      } else {
        elements.push(<span key={idx++}>{word}</span>);
      }
    } else if (newline) {
      elements.push(<br key={idx++} />);
    } else {
      elements.push(<span key={idx++}>{full}</span>);
    }
  }

  return elements;
}

export function SqlViewer({ sql }: SqlViewerProps) {
  const highlighted = useMemo(() => {
    const formatted = formatSql(sql);
    return highlightSql(formatted);
  }, [sql]);

  return (
    <div className="bg-gray-950 rounded-lg overflow-auto">
      <div className="flex items-center justify-between px-4 py-2 border-b border-gray-800">
        <span className="text-[10px] uppercase tracking-wider text-gray-500 font-medium">SQL</span>
        <button
          onClick={() => navigator.clipboard.writeText(sql)}
          className="text-[10px] text-gray-500 hover:text-gray-300 transition-colors"
        >
          Copy
        </button>
      </div>
      <pre className="p-4 text-sm font-mono leading-relaxed text-gray-200 whitespace-pre-wrap">
        <code>{highlighted}</code>
      </pre>
    </div>
  );
}
