"use client";

import { useEffect } from "react";
import { useParams, useRouter } from "next/navigation";

// Project root redirects to understand stage
export default function ProjectPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  useEffect(() => {
    router.replace(`/h2/projects/${id}/understand`);
  }, [id, router]);

  return null;
}
