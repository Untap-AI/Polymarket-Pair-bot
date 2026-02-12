"use client";

/* eslint-disable @typescript-eslint/no-explicit-any */

import * as React from "react";
import {
  LineChart,
  Line,
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

interface DailyPoint {
  date: string;
  attempts: number;
  pairs: number;
  pair_rate: number | null;
  avg_profit: number | null;
  total_pnl: number;
}

interface HourlyPoint {
  hour: number;
  attempts: number;
  pairs: number;
  pair_rate: number | null;
  avg_profit: number | null;
  total_pnl: number;
}

interface TimeSeriesChartProps {
  daily: DailyPoint[];
  hourly: HourlyPoint[];
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

export function TimeSeriesChart({ daily, hourly }: TimeSeriesChartProps) {
  const [metric, setMetric] = React.useState<Metric>("pair_rate");
  const config = METRIC_CONFIG[metric];

  const dailyData = daily.map((d) => ({
    ...d,
    date:
      typeof d.date === "string"
        ? d.date.slice(0, 10)
        : new Date(d.date).toISOString().slice(0, 10),
  }));

  const hourlyData = hourly.map((h) => ({
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
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Daily Trend */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Daily Trend: {config.label}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {dailyData.length === 0 ? (
              <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">
                No data for selected filters
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={dailyData}>
                  <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                  <XAxis
                    dataKey="date"
                    tick={{ fontSize: 11 }}
                    tickFormatter={(v) =>
                      typeof v === "string" ? v.slice(5) : ""
                    }
                  />
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
                    labelFormatter={(label: any) => `Date: ${label}`}
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey={metric}
                    stroke={config.color}
                    name={config.label}
                    strokeWidth={2}
                    dot={{ r: 2 }}
                    activeDot={{ r: 4 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* Hourly Pattern */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Hourly Pattern: {config.label}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {hourlyData.length === 0 ? (
              <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">
                No data for selected filters
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
    </div>
  );
}
