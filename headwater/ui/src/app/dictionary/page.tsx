"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function DictionaryPage() {
  const router = useRouter();

  useEffect(() => {
    router.replace("/discovery");
  }, [router]);

  return (
    <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted">
      Opening Discover & Access...
    </div>
  );
}
