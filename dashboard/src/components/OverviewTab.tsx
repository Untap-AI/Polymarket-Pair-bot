"use client";

import * as React from "react";
import { KpiCards } from "@/components/KpiCards";
import { ProjectionPanel } from "@/components/ProjectionPanel";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";

/* eslint-disable @typescript-eslint/no-explicit-any */

interface OverviewTabProps {
  filters: FilterParams;
}

export function OverviewTab({ filters }: OverviewTabProps) {
  const [stats, setStats] = React.useState<any>(null);
  const [projection, setProjection] = React.useState<any>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const requestIdRef = React.useRef(0);
  React.useEffect(() => {
    const id = ++requestIdRef.current;
    setLoading(true);
    setError(null);
    const qs = filtersToSearchParams(filters);
    fetch(`/api/stats?${qs}`)
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch stats");
        return res.json();
      })
      .then((data) => {
        if (requestIdRef.current !== id) return;
        setStats(data.stats);
        setProjection(data.projection);
      })
      .catch((err) => {
        if (requestIdRef.current !== id) return;
        setError(err instanceof Error ? err.message : "Unknown error");
        console.error("OverviewTab fetch error:", err);
      })
      .finally(() => {
        if (requestIdRef.current === id) setLoading(false);
      });
  }, [filters]);

  return (
    <div className="space-y-6">
      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
          Error: {error}
        </div>
      )}

      {loading && !stats && (
        <p className="text-sm text-muted-foreground animate-pulse">
          Loading overview...
        </p>
      )}

      {loading && stats && (
        <p className="text-sm text-muted-foreground animate-pulse">
          Updating...
        </p>
      )}

      <KpiCards stats={stats} projection={projection} />

      <ProjectionPanel projection={projection} />
    </div>
  );
}
