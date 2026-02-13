"use client";

import * as React from "react";
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

interface BreakdownTableProps {
  filters: FilterParams;
}

const TABS = [
  { key: "delta", label: "By Delta" },
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
  const [data, setData] = React.useState<BreakdownRow[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [sortKey, setSortKey] = React.useState<SortKey>("attempts");
  const [sortDir, setSortDir] = React.useState<SortDir>("desc");

  const fetchData = React.useCallback(
    async (groupBy: string) => {
      setLoading(true);
      try {
        const qs = filtersToSearchParams(filters);
        const sep = qs ? "&" : "";
        const res = await fetch(
          `/api/breakdown?groupBy=${groupBy}${sep}${qs}`
        );
        if (res.ok) {
          const rows = await res.json();
          setData(rows);
        }
      } catch (err) {
        console.error("Failed to fetch breakdown:", err);
      } finally {
        setLoading(false);
      }
    },
    [filters]
  );

  React.useEffect(() => {
    fetchData(activeTab);
  }, [activeTab, fetchData]);

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
