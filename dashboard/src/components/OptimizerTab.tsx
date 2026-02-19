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
  ReferenceLine,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";

// ---------------------------------------------------------------
// Types
// ---------------------------------------------------------------

interface RankingRow {
  delta_points: number | null;
  s0_points: number | null;
  stop_loss: number | null;
  attempts: number;
  markets: number;
  att_per_mkt: number;
  pairs: number;
  stopped: number;
  pair_rate: number | null;
  avg_pnl: number | null;
  total_pnl: number;
  pnl_per_mkt: number;
}

interface EnvBucket {
  bucket: string;
  attempts: number;
  markets: number;
  avg_pnl: number | null;
  pnl_per_mkt: number | null;
}

type EnvDimension = "spread" | "priceRegime" | "timeRemaining";

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function formatPct(val: number | null): string {
  if (val === null || val === undefined) return "-";
  return `${(val * 100).toFixed(1)}%`;
}

function formatNum(val: number | null, decimals = 1): string {
  if (val === null || val === undefined) return "-";
  return val.toFixed(decimals);
}

function slLabel(val: number | null): string {
  return val !== null ? String(val) : "-";
}

function comboLabel(row: RankingRow): string {
  const parts = [`δ${row.delta_points}`, `S0=${row.s0_points}`];
  if (row.stop_loss !== null) parts.push(`SL=${row.stop_loss}`);
  return parts.join(" ");
}

// ---------------------------------------------------------------
// Component
// ---------------------------------------------------------------

interface OptimizerTabProps {
  filters: FilterParams;
}

const ENV_TABS: { key: EnvDimension; label: string }[] = [
  { key: "spread", label: "Spread" },
  { key: "priceRegime", label: "Price Regime" },
  { key: "timeRemaining", label: "Time Remaining" },
];

export function OptimizerTab({ filters }: OptimizerTabProps) {
  // Ranking data
  const [ranking, setRanking] = React.useState<RankingRow[]>([]);
  const [loading, setLoading] = React.useState(false);

  // Selected combo for environmental breakdown
  const [selectedIdx, setSelectedIdx] = React.useState<number | null>(null);
  const [envDim, setEnvDim] = React.useState<EnvDimension>("spread");
  const [envData, setEnvData] = React.useState<EnvBucket[]>([]);
  const [envLoading, setEnvLoading] = React.useState(false);

  // Fetch ranking
  const rankReqRef = React.useRef(0);
  React.useEffect(() => {
    const id = ++rankReqRef.current;
    setLoading(true);
    const qs = filtersToSearchParams(filters);
    fetch(`/api/optimizer?${qs}`)
      .then((res) => (res.ok ? res.json() : []))
      .then((rows: RankingRow[]) => {
        if (rankReqRef.current === id) {
          setRanking(rows);
          setSelectedIdx(rows.length > 0 ? 0 : null);
        }
      })
      .catch((err) => {
        if (rankReqRef.current === id)
          console.error("Optimizer ranking fetch error:", err);
      })
      .finally(() => {
        if (rankReqRef.current === id) setLoading(false);
      });
  }, [filters]);

  // Fetch env breakdown when selected combo or dimension changes
  const envReqRef = React.useRef(0);
  React.useEffect(() => {
    if (selectedIdx === null || !ranking[selectedIdx]) {
      setEnvData([]);
      return;
    }

    const id = ++envReqRef.current;
    setEnvLoading(true);
    const combo = ranking[selectedIdx];
    const qs = filtersToSearchParams(filters);
    const sep = qs ? "&" : "";
    const slParam = combo.stop_loss !== null ? combo.stop_loss : "null";
    const envQs =
      `env=${envDim}&delta=${combo.delta_points}&s0=${combo.s0_points}&stopLoss=${slParam}`;
    fetch(`/api/optimizer?${envQs}${sep}${qs}`)
      .then((res) => (res.ok ? res.json() : []))
      .then((rows: EnvBucket[]) => {
        if (envReqRef.current === id) setEnvData(rows);
      })
      .catch((err) => {
        if (envReqRef.current === id)
          console.error("Env breakdown fetch error:", err);
      })
      .finally(() => {
        if (envReqRef.current === id) setEnvLoading(false);
      });
  }, [selectedIdx, envDim, ranking, filters]);

  const selectedCombo = selectedIdx !== null ? ranking[selectedIdx] : null;
  const baselinePnl = selectedCombo?.pnl_per_mkt ?? 0;

  // Chart data with vs-baseline coloring
  const chartData = envData.map((b) => ({
    ...b,
    vs_baseline: (b.pnl_per_mkt ?? 0) - baselinePnl,
  }));

  return (
    <div className="space-y-6">
      {/* ---- Ranking Table ---- */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">
            Parameter Combo Ranking
            <span className="ml-2 text-xs font-normal text-muted-foreground">
              sorted by PNL / market (attempts x avg PNL per attempt)
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center h-32 text-muted-foreground">
              Loading...
            </div>
          ) : ranking.length === 0 ? (
            <div className="flex items-center justify-center h-32 text-muted-foreground text-sm">
              No data for selected filters
            </div>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-10">#</TableHead>
                    <TableHead>Combo</TableHead>
                    <TableHead className="text-right">Attempts</TableHead>
                    <TableHead className="text-right">Markets</TableHead>
                    <TableHead className="text-right">Att/Mkt</TableHead>
                    <TableHead className="text-right">Pair Rate</TableHead>
                    <TableHead className="text-right">Avg PnL</TableHead>
                    <TableHead className="text-right">PnL/Mkt</TableHead>
                    <TableHead className="text-right">Total PnL</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {ranking.map((row, i) => {
                    const isTop5 = i < 5;
                    const isSelected = i === selectedIdx;
                    return (
                      <TableRow
                        key={i}
                        className={`cursor-pointer transition-colors ${
                          isSelected
                            ? "bg-primary/10"
                            : "hover:bg-muted/50"
                        }`}
                        onClick={() => setSelectedIdx(i)}
                      >
                        <TableCell className="font-mono text-xs">
                          {isTop5 ? (
                            <Badge
                              variant="default"
                              className="text-[10px] px-1.5 py-0"
                            >
                              {i + 1}
                            </Badge>
                          ) : (
                            <span className="text-muted-foreground">
                              {i + 1}
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="font-medium whitespace-nowrap">
                          <span className="font-mono text-xs">
                            δ{row.delta_points} S0={row.s0_points} SL=
                            {slLabel(row.stop_loss)}
                          </span>
                        </TableCell>
                        <TableCell className="text-right">
                          {row.attempts.toLocaleString()}
                        </TableCell>
                        <TableCell className="text-right">
                          {row.markets.toLocaleString()}
                        </TableCell>
                        <TableCell className="text-right">
                          {formatNum(row.att_per_mkt)}
                        </TableCell>
                        <TableCell className="text-right">
                          {formatPct(row.pair_rate)}
                        </TableCell>
                        <TableCell
                          className={`text-right ${
                            (row.avg_pnl ?? 0) >= 0
                              ? "text-emerald-600 dark:text-emerald-400"
                              : "text-red-600 dark:text-red-400"
                          }`}
                        >
                          {formatNum(row.avg_pnl, 2)} pts
                        </TableCell>
                        <TableCell
                          className={`text-right font-semibold ${
                            row.pnl_per_mkt >= 0
                              ? "text-emerald-600 dark:text-emerald-400"
                              : "text-red-600 dark:text-red-400"
                          }`}
                        >
                          {formatNum(row.pnl_per_mkt, 2)} pts
                        </TableCell>
                        <TableCell
                          className={`text-right ${
                            row.total_pnl >= 0
                              ? "text-emerald-600 dark:text-emerald-400"
                              : "text-red-600 dark:text-red-400"
                          }`}
                        >
                          {formatNum(row.total_pnl, 0)} pts
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ---- Environmental Breakdown ---- */}
      {selectedCombo && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Environmental Breakdown:{" "}
              <span className="font-mono">{comboLabel(selectedCombo)}</span>
              <span className="ml-2 text-xs font-normal text-muted-foreground">
                baseline PnL/mkt = {formatNum(selectedCombo.pnl_per_mkt, 2)} pts
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Tabs
              value={envDim}
              onValueChange={(v) => setEnvDim(v as EnvDimension)}
            >
              <TabsList className="mb-4">
                {ENV_TABS.map((t) => (
                  <TabsTrigger key={t.key} value={t.key} className="text-xs">
                    {t.label}
                  </TabsTrigger>
                ))}
              </TabsList>
            </Tabs>

            {envLoading ? (
              <div className="flex items-center justify-center h-48 text-muted-foreground">
                Loading...
              </div>
            ) : chartData.length === 0 ? (
              <div className="flex items-center justify-center h-48 text-muted-foreground text-sm">
                No data
              </div>
            ) : (
              <>
                {/* Bar chart */}
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart
                    data={chartData}
                    margin={{ top: 5, right: 5, left: 5, bottom: 5 }}
                  >
                    <CartesianGrid
                      strokeDasharray="3 3"
                      className="opacity-30"
                    />
                    <XAxis dataKey="bucket" tick={{ fontSize: 10 }} />
                    <YAxis
                      tick={{ fontSize: 10 }}
                      tickFormatter={(v) => `${Math.round(v)}`}
                    />
                    <Tooltip
                      formatter={(value: any, name: any) => {
                        if (name === "pnl_per_mkt")
                          return [`${Number(value).toFixed(2)} pts`, "PnL/Mkt"];
                        return [value, name];
                      }}
                      labelFormatter={(label: any) => `${label}`}
                    />
                    <ReferenceLine
                      y={baselinePnl}
                      stroke="#64748b"
                      strokeDasharray="3 3"
                      label={{
                        value: "baseline",
                        position: "right",
                        fontSize: 10,
                      }}
                    />
                    <Bar
                      dataKey="pnl_per_mkt"
                      name="pnl_per_mkt"
                      radius={[3, 3, 0, 0]}
                      fill="#6366f1"
                    />
                  </BarChart>
                </ResponsiveContainer>

                {/* Detail table */}
                <div className="overflow-x-auto mt-4">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Bucket</TableHead>
                        <TableHead className="text-right">Attempts</TableHead>
                        <TableHead className="text-right">Markets</TableHead>
                        <TableHead className="text-right">Avg PnL</TableHead>
                        <TableHead className="text-right">PnL/Mkt</TableHead>
                        <TableHead className="text-right">vs Baseline</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {chartData.map((b, i) => (
                        <TableRow key={i}>
                          <TableCell className="font-medium">{b.bucket}</TableCell>
                          <TableCell className="text-right">
                            {b.attempts.toLocaleString()}
                          </TableCell>
                          <TableCell className="text-right">
                            {b.markets.toLocaleString()}
                          </TableCell>
                          <TableCell className="text-right">
                            {formatNum(b.avg_pnl, 2)} pts
                          </TableCell>
                          <TableCell
                            className={`text-right font-medium ${
                              (b.pnl_per_mkt ?? 0) >= 0
                                ? "text-emerald-600 dark:text-emerald-400"
                                : "text-red-600 dark:text-red-400"
                            }`}
                          >
                            {formatNum(b.pnl_per_mkt, 2)} pts
                          </TableCell>
                          <TableCell
                            className={`text-right ${
                              b.vs_baseline >= 0
                                ? "text-emerald-600 dark:text-emerald-400"
                                : "text-red-600 dark:text-red-400"
                            }`}
                          >
                            {b.vs_baseline >= 0 ? "+" : ""}
                            {formatNum(b.vs_baseline, 2)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
