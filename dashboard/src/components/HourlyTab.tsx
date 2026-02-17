"use client";

/* eslint-disable @typescript-eslint/no-explicit-any */

import * as React from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";

interface HourlyPoint {
  hour: number;
  attempts: number;
  pairs: number;
  pair_rate: number | null;
  avg_profit: number | null;
  total_pnl: number;
}

type Metric = "pair_rate" | "avg_profit" | "attempts" | "total_pnl";

const METRIC_CONFIG: Record<
  Metric,
  { label: string; color: string; formatter: (v: number) => string }
> = {
  pair_rate: {
    label: "Pair Rate",
    color: "#10b981",
    formatter: (v) => `${(v * 100).toFixed(1)}%`,
  },
  avg_profit: {
    label: "Avg Profit (pts)",
    color: "#6366f1",
    formatter: (v) => `${v.toFixed(1)} pts`,
  },
  attempts: {
    label: "Attempts",
    color: "#f59e0b",
    formatter: (v) => String(Math.round(v)),
  },
  total_pnl: {
    label: "Total P&L (pts)",
    color: "#ef4444",
    formatter: (v) => `${v} pts`,
  },
};

interface HourlyTabProps {
  filters: FilterParams;
}

export function HourlyTab({ filters }: HourlyTabProps) {
  const [metric, setMetric] = React.useState<Metric>("pair_rate");
  const [data, setData] = React.useState<HourlyPoint[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const requestIdRef = React.useRef(0);
  React.useEffect(() => {
    const id = ++requestIdRef.current;
    setLoading(true);
    setError(null);
    const qs = filtersToSearchParams(filters);
    fetch(`/api/hourly?${qs}`)
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch hourly data");
        return res.json();
      })
      .then((rows: HourlyPoint[]) => {
        if (requestIdRef.current === id) setData(rows);
      })
      .catch((err) => {
        if (requestIdRef.current === id) {
          setError(err instanceof Error ? err.message : "Unknown error");
          console.error("HourlyTab fetch error:", err);
        }
      })
      .finally(() => {
        if (requestIdRef.current === id) setLoading(false);
      });
  }, [filters]);

  const config = METRIC_CONFIG[metric];

  const hourlyData = data.map((h) => ({
    ...h,
    hourLabel: `${String(h.hour).padStart(2, "0")}:00`,
  }));

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <Tabs
          value={metric}
          onValueChange={(v) => setMetric(v as Metric)}
        >
          <TabsList>
            <TabsTrigger value="pair_rate">Pair Rate</TabsTrigger>
            <TabsTrigger value="avg_profit">Profit</TabsTrigger>
            <TabsTrigger value="attempts">Volume</TabsTrigger>
            <TabsTrigger value="total_pnl">P&L</TabsTrigger>
          </TabsList>
        </Tabs>
        {loading && (
          <span className="text-sm text-muted-foreground animate-pulse">
            Loading...
          </span>
        )}
      </div>

      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
          Error: {error}
        </div>
      )}

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">
            Hourly Pattern: {config.label}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {hourlyData.length === 0 ? (
            <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">
              {loading ? "Loading..." : "No data for selected filters"}
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={hourlyData}>
                <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                <XAxis dataKey="hourLabel" tick={{ fontSize: 11 }} />
                <YAxis
                  tick={{ fontSize: 11 }}
                  tickFormatter={(v) =>
                    metric === "pair_rate"
                      ? `${(v * 100).toFixed(0)}%`
                      : String(Math.round(v))
                  }
                />
                <Tooltip
                  formatter={(value: any) => [
                    config.formatter(Number(value)),
                    config.label,
                  ]}
                  labelFormatter={(label: any) => `Hour: ${label}`}
                />
                <Legend />
                <Bar
                  dataKey={metric}
                  fill={config.color}
                  name={config.label}
                  radius={[2, 2, 0, 0]}
                />
              </BarChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
