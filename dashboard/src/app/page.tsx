"use client";

import * as React from "react";
import { FilterSidebar, type FilterOptions } from "@/components/FilterSidebar";
import { OverviewTab } from "@/components/OverviewTab";
import { HourlyTab } from "@/components/HourlyTab";
import { BreakdownTable } from "@/components/BreakdownTable";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import type { FilterParams } from "@/lib/filters";

function fiveDaysAgo(): string {
  const d = new Date();
  d.setDate(d.getDate() - 5);
  return d.toISOString().slice(0, 10);
}

const DEFAULT_FILTERS: FilterParams = { dateAfter: fiveDaysAgo() };

type DashboardTab = "overview" | "hourly" | "breakdown";

export default function DashboardPage() {
  // Filter state
  const [filterOptions, setFilterOptions] =
    React.useState<FilterOptions | null>(null);
  const [filters, setFilters] = React.useState<FilterParams>(DEFAULT_FILTERS);
  const [appliedFilters, setAppliedFilters] =
    React.useState<FilterParams | null>(null);

  // UI state
  const [activeTab, setActiveTab] = React.useState<DashboardTab>("overview");
  const [sidebarOpen, setSidebarOpen] = React.useState(true);

  // Load filter options on mount; once loaded, default to first delta to reduce initial query load
  React.useEffect(() => {
    fetch("/api/filters")
      .then((r) => {
        if (!r.ok) throw new Error(`Filter options returned ${r.status}`);
        return r.json();
      })
      .then((data) => {
        setFilterOptions(data);
        const firstDelta = data.deltaPoints?.[0];
        const withDelta =
          firstDelta != null
            ? { ...DEFAULT_FILTERS, deltaPoints: [firstDelta] }
            : DEFAULT_FILTERS;
        setFilters(withDelta);
      })
      .catch((err) => {
        console.error("Failed to load filter options:", err);
      });
  }, []);

  const handleApply = () => {
    const cleaned: FilterParams = {};
    for (const [key, value] of Object.entries(filters)) {
      if (Array.isArray(value) && value.length === 0) continue;
      if (value === undefined) continue;
      if (key === "hourRange") {
        const hr = value as [number, number];
        if (hr[0] === 0 && hr[1] === 23) continue;
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (cleaned as any)[key] = value;
    }
    setAppliedFilters(cleaned);
  };

  const handleReset = () => {
    const firstDelta = filterOptions?.deltaPoints?.[0];
    const defaults =
      firstDelta != null
        ? { dateAfter: fiveDaysAgo(), deltaPoints: [firstDelta] }
        : { dateAfter: fiveDaysAgo() };
    setFilters(defaults);
    setAppliedFilters(defaults);
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
        </header>

        {/* Tab navigation */}
        <div className="px-6 pt-4">
          <Tabs
            value={activeTab}
            onValueChange={(v) => setActiveTab(v as DashboardTab)}
          >
            <TabsList>
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="hourly">Hourly Pattern</TabsTrigger>
              <TabsTrigger value="breakdown">Breakdown</TabsTrigger>
            </TabsList>
          </Tabs>
        </div>

        {/* Tab content - wait for filter options; data loads only after Apply */}
        <div className="p-6">
          {!filterOptions ? (
            <div className="flex items-center justify-center py-12 text-sm text-muted-foreground">
              Loading filters...
            </div>
          ) : appliedFilters === null ? (
            <div className="flex flex-col items-center justify-center py-16 text-sm text-muted-foreground gap-2">
              <p>Select filters and click Apply to load data.</p>
            </div>
          ) : (
            <>
              {activeTab === "overview" && (
                <OverviewTab filters={appliedFilters} />
              )}
              {activeTab === "hourly" && (
                <HourlyTab filters={appliedFilters} />
              )}
              {activeTab === "breakdown" && (
                <BreakdownTable filters={appliedFilters} />
              )}
            </>
          )}
        </div>
      </main>
    </div>
  );
}
