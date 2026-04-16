"use client";

/**
 * Consistent confidence indicator used across all pages.
 * Green >= 0.8, Yellow >= 0.5, Orange/Red < 0.5.
 */
export function ConfidenceDot({ value }: { value: number }) {
  const color =
    value >= 0.8
      ? "bg-green-500"
      : value >= 0.5
        ? "bg-yellow-500"
        : "bg-orange-500";
  return (
    <span
      className={`inline-block w-2 h-2 rounded-full shrink-0 ${color}`}
      title={`${(value * 100).toFixed(0)}% confidence`}
    />
  );
}
