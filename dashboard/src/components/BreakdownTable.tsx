"use client";

/* eslint-disable @typescript-eslint/no-explicit-any */

import * as React from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";

interface BreakdownRow {
  group_key: string | number | null;
  attempts: number;
  pairs: number;
  failed: number;
  stopped: number;
  pair_rate: number | null;
  avg_ttp: number | null;
  avg_profit: number | null;
  total_pnl: number;
  avg_mae: number | null;
}

type SortKey = keyof BreakdownRow;
type SortDir = "asc" | "desc";

type ChartMetric = "attempts" | "pairs" | "pair_rate" | "total_pnl" | "avg_profit";

const CHART_METRIC_CONFIG: Record<
  ChartMetric,
  { label: string; color: string; formatter: (v: number) => string }
> = {
  attempts: {
    label: "Attempts",
    color: "#f59e0b",
    formatter: (v) => String(Math.round(v)),
  },
  pairs: {
    label: "Pairs",
    color: "#10b981",
    formatter: (v) => String(Math.round(v)),
  },
  pair_rate: {
    label: "Pair Rate",
    color: "#6366f1",
    formatter: (v) => `${(v * 100).toFixed(1)}%`,
  },
  total_pnl: {
    label: "Total P&L (pts)",
    color: "#ef4444",
    formatter: (v) => `${v} pts`,
  },
  avg_profit: {
    label: "Avg Profit (pts)",
    color: "#8b5cf6",
    formatter: (v) => `${v.toFixed(1)} pts`,
  },
};

interface BreakdownTableProps {
  filters: FilterParams;
}

const TABS = [
  { key: "delta", label: "By Delta" },
  { key: "s0", label: "By S0" },
  { key: "stopLoss", label: "By Stop Loss" },
  { key: "asset", label: "By Asset" },
  { key: "timeRemaining", label: "By Time Remaining" },
  { key: "combinedSpread", label: "By Spread" },
  { key: "priceRegime", label: "By Price Regime" },
  { key: "firstLeg", label: "By First Leg" },
  { key: "dayOfWeek", label: "By Day of Week" },
  { key: "hourOfDay", label: "By Hour" },
  { key: "p1Cost", label: "By First Leg Cost" },
  { key: "parameterSet", label: "By Param Set" },
] as const;

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function formatGroupKey(groupBy: string, key: string | number | null): string {
  if (key === null || key === undefined) return "N/A";
  if (groupBy === "dayOfWeek") return DAY_NAMES[Number(key)] || String(key);
  if (groupBy === "hourOfDay") return `${String(key).padStart(2, "0")}:00`;
  if (groupBy === "p1Cost") return `${key}¢`;
  return String(key);
}

function formatPct(val: number | null): string {
  if (val === null || val === undefined) return "-";
  return `${(val * 100).toFixed(1)}%`;
}

function formatNum(val: number | null, decimals = 1): string {
  if (val === null || val === undefined) return "-";
  return val.toFixed(decimals);
}

export function BreakdownTable({ filters }: BreakdownTableProps) {
  const [activeTab, setActiveTab] = React.useState("delta");
  const [chartMetric, setChartMetric] = React.useState<ChartMetric>("total_pnl");
  const [data, setData] = React.useState<BreakdownRow[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [sortKey, setSortKey] = React.useState<SortKey>("attempts");
  const [sortDir, setSortDir] = React.useState<SortDir>("desc");

  const requestIdRef = React.useRef(0);
  React.useEffect(() => {
    const id = ++requestIdRef.current;
    setLoading(true);
    const qs = filtersToSearchParams(filters);
    const sep = qs ? "&" : "";
    fetch(`/api/breakdown?groupBy=${activeTab}${sep}${qs}`)
      .then((res) => (res.ok ? res.json() : []))
      .then((rows) => {
        if (requestIdRef.current === id) setData(rows);
      })
      .catch((err) => {
        if (requestIdRef.current === id) {
          console.error("Failed to fetch breakdown:", err);
        }
      })
      .finally(() => {
        if (requestIdRef.current === id) setLoading(false);
      });
  }, [activeTab, filters]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const sortedData = React.useMemo(() => {
    return [...data].sort((a, b) => {
      const aVal = a[sortKey] ?? -Infinity;
      const bVal = b[sortKey] ?? -Infinity;
      if (aVal < bVal) return sortDir === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
  }, [data, sortKey, sortDir]);

  // Chart data: sort by group_key for natural X order (delta, hour, day, p1Cost)
  const chartData = React.useMemo(() => {
    const numeric = ["delta", "s0", "stopLoss", "hourOfDay", "dayOfWeek", "p1Cost"];
    const sorted = [...data];
    if (numeric.includes(activeTab)) {
      sorted.sort((a, b) => {
        const aNum = Number(a.group_key);
        const bNum = Number(b.group_key);
        if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) return aNum - bNum;
        return String(a.group_key).localeCompare(String(b.group_key));
      });
    }
    return sorted.map((row) => ({
      name: formatGroupKey(activeTab, row.group_key),
      [chartMetric]: row[chartMetric] ?? 0,
      ...row,
    }));
  }, [data, activeTab, chartMetric]);

  const SortHeader = ({
    label,
    field,
    className,
  }: {
    label: string;
    field: SortKey;
    className?: string;
  }) => (
    <TableHead
      className={`cursor-pointer select-none hover:bg-muted/50 ${className || ""}`}
      onClick={() => handleSort(field)}
    >
      <div className="flex items-center gap-1">
        {label}
        {sortKey === field && (
          <span className="text-xs">{sortDir === "asc" ? "^" : "v"}</span>
        )}
      </div>
    </TableHead>
  );

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium">
          Breakdown Analysis
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="flex flex-wrap h-auto gap-1 mb-4">
            {TABS.map((tab) => (
              <TabsTrigger key={tab.key} value={tab.key} className="text-xs">
                {tab.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>

        {chartData.length > 0 && (
          <div className="mb-6">
            <Tabs value={chartMetric} onValueChange={(v) => setChartMetric(v as ChartMetric)}>
              <TabsList className="mb-3">
                <TabsTrigger value="attempts">Attempts</TabsTrigger>
                <TabsTrigger value="pairs">Pairs</TabsTrigger>
                <TabsTrigger value="pair_rate">Pair Rate</TabsTrigger>
                <TabsTrigger value="total_pnl">P&L</TabsTrigger>
                <TabsTrigger value="avg_profit">Avg Profit</TabsTrigger>
              </TabsList>
            </Tabs>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={chartData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                <YAxis
                  tick={{ fontSize: 10 }}
                  tickFormatter={(v) =>
                    chartMetric === "pair_rate"
                      ? `${(v * 100).toFixed(0)}%`
                      : String(Math.round(v))
                  }
                />
                <Tooltip
                  formatter={(value: any) => [
                    CHART_METRIC_CONFIG[chartMetric].formatter(Number(value)),
                    CHART_METRIC_CONFIG[chartMetric].label,
                  ]}
                  labelFormatter={(label: any) => `Group: ${label}`}
                />
                {chartMetric === "total_pnl" && (
                  <ReferenceLine y={0} stroke="#64748b" strokeDasharray="3 3" />
                )}
                <Line
                  type="monotone"
                  dataKey={chartMetric}
                  stroke={CHART_METRIC_CONFIG[chartMetric].color}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  activeDot={{ r: 5 }}
                  name={CHART_METRIC_CONFIG[chartMetric].label}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {loading ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground">
            Loading...
          </div>
        ) : sortedData.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground text-sm">
            No data for selected filters
          </div>
        ) : (
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <SortHeader label="Group" field="group_key" />
                  <SortHeader label="Attempts" field="attempts" className="text-right" />
                  <SortHeader label="Pairs" field="pairs" className="text-right" />
                  <SortHeader label="Pair Rate" field="pair_rate" className="text-right" />
                  <SortHeader label="Avg TTP" field="avg_ttp" className="text-right" />
                  <SortHeader label="Avg Profit" field="avg_profit" className="text-right" />
                  <SortHeader label="Total P&L" field="total_pnl" className="text-right" />
                  <SortHeader label="Avg MAE" field="avg_mae" className="text-right" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedData.map((row, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-medium">
                      {formatGroupKey(activeTab, row.group_key)}
                    </TableCell>
                    <TableCell className="text-right">
                      {row.attempts.toLocaleString()}
                    </TableCell>
                    <TableCell className="text-right">
                      {row.pairs.toLocaleString()}
                    </TableCell>
                    <TableCell className="text-right">
                      {formatPct(row.pair_rate)}
                    </TableCell>
                    <TableCell className="text-right">
                      {formatNum(row.avg_ttp)}s
                    </TableCell>
                    <TableCell className="text-right">
                      {formatNum(row.avg_profit)} pts
                    </TableCell>
                    <TableCell
                      className={`text-right font-medium ${
                        row.total_pnl >= 0
                          ? "text-emerald-600 dark:text-emerald-400"
                          : "text-red-600 dark:text-red-400"
                      }`}
                    >
                      {row.total_pnl} pts
                    </TableCell>
                    <TableCell className="text-right">
                      {formatNum(row.avg_mae)} pts
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
