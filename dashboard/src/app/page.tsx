"use client";

import * as React from "react";
import { FilterSidebar, type FilterOptions } from "@/components/FilterSidebar";
import { KpiCards } from "@/components/KpiCards";
import { TimeSeriesChart } from "@/components/TimeSeriesChart";
import { FirstLegPnlChart } from "@/components/FirstLegPnlChart";
import { HourOfDayPnlChart } from "@/components/HourOfDayPnlChart";
import { BreakdownTable } from "@/components/BreakdownTable";
import { ProjectionPanel } from "@/components/ProjectionPanel";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";
import { Button } from "@/components/ui/button";

/* eslint-disable @typescript-eslint/no-explicit-any */

const EMPTY_FILTERS: FilterParams = {};

export default function DashboardPage() {
  // Filter state
  const [filterOptions, setFilterOptions] = React.useState<FilterOptions | null>(null);
  const [filters, setFilters] = React.useState<FilterParams>(EMPTY_FILTERS);
  const [appliedFilters, setAppliedFilters] = React.useState<FilterParams>(EMPTY_FILTERS);

  // Data state
  const [stats, setStats] = React.useState<any>(null);
  const [projection, setProjection] = React.useState<any>(null);
  const [timeseries, setTimeseries] = React.useState<{
    daily: any[];
    hourly: any[];
  }>({ daily: [], hourly: [] });

  // UI state
  const [loading, setLoading] = React.useState(false);
  const [sidebarOpen, setSidebarOpen] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  // Load filter options on mount
  React.useEffect(() => {
    fetch("/api/filters")
      .then((r) => r.json())
      .then(setFilterOptions)
      .catch((err) => console.error("Failed to load filter options:", err));
  }, []);

  // Fetch data function
  const fetchData = React.useCallback(async (f: FilterParams) => {
    setLoading(true);
    setError(null);
    try {
      const qs = filtersToSearchParams(f);
      const [statsRes, tsRes] = await Promise.all([
        fetch(`/api/stats?${qs}`),
        fetch(`/api/timeseries?${qs}`),
      ]);

      if (!statsRes.ok || !tsRes.ok) {
        throw new Error("Failed to fetch data");
      }

      const statsData = await statsRes.json();
      const tsData = await tsRes.json();

      setStats(statsData.stats);
      setProjection(statsData.projection);
      setTimeseries(tsData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
      console.error("Error fetching data:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch data on mount
  React.useEffect(() => {
    fetchData(EMPTY_FILTERS);
  }, [fetchData]);

  const handleApply = () => {
    // Clean up filters: remove empty arrays and undefined values
    const cleaned: FilterParams = {};
    for (const [key, value] of Object.entries(filters)) {
      if (Array.isArray(value) && value.length === 0) continue;
      if (value === undefined) continue;
      // Skip hour range if it's the full range
      if (key === "hourRange") {
        const hr = value as [number, number];
        if (hr[0] === 0 && hr[1] === 23) continue;
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (cleaned as any)[key] = value;
    }
    setAppliedFilters(cleaned);
    fetchData(cleaned);
  };

  const handleReset = () => {
    setFilters(EMPTY_FILTERS);
    setAppliedFilters(EMPTY_FILTERS);
    fetchData(EMPTY_FILTERS);
  };

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      {/* Sidebar */}
      <aside
        className={`${
          sidebarOpen ? "w-72" : "w-0"
        } transition-all duration-200 border-r overflow-y-auto overflow-x-hidden shrink-0`}
      >
        {filterOptions && sidebarOpen && (
          <FilterSidebar
            options={filterOptions}
            filters={filters}
            onChange={setFilters}
            onApply={handleApply}
            onReset={handleReset}
            loading={loading}
          />
        )}
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">
        {/* Header */}
        <header className="sticky top-0 z-10 bg-background/95 backdrop-blur border-b px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="h-8 w-8 p-0"
            >
              <svg
                className="h-4 w-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d={sidebarOpen ? "M11 19l-7-7 7-7" : "M13 5l7 7-7 7"}
                />
              </svg>
            </Button>
            <h1 className="text-lg font-semibold">
              Pair Analytics Dashboard
            </h1>
          </div>
          {loading && (
            <span className="text-sm text-muted-foreground animate-pulse">
              Loading data...
            </span>
          )}
        </header>

        {/* Content */}
        <div className="p-6 space-y-6">
          {error && (
            <div className="rounded-md bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
              Error: {error}. Make sure DATABASE_URL is set in
              dashboard/.env.local
            </div>
          )}

          {/* KPI Cards */}
          <KpiCards stats={stats} projection={projection} />

          {/* Time Series Charts */}
          <TimeSeriesChart
            daily={timeseries.daily}
            hourly={timeseries.hourly}
          />

          {/* P&L by First Leg Cost & Hour of Day */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <FirstLegPnlChart filters={appliedFilters} />
            <HourOfDayPnlChart filters={appliedFilters} />
          </div>

          {/* Breakdown + Projection side by side */}
          <div className="grid grid-cols-1 xl:grid-cols-[1fr_320px] gap-6">
            <BreakdownTable filters={appliedFilters} />
            <ProjectionPanel projection={projection} />
          </div>
        </div>
      </main>
    </div>
  );
}
