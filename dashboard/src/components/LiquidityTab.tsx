"use client";

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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";

interface SizeRow {
  bucket: string;
  attempts: number;
  pairs: number;
  pair_rate: number | null;
  avg_pnl: number | null;
  avg_size: number | null;
}

interface DepthRow {
  bucket: string;
  attempts: number;
  pairs: number;
  pair_rate: number | null;
  avg_pnl: number | null;
  avg_depth: number | null;
}

interface MarketRow {
  bucket: string;
  attempts: number;
  markets: number;
  pair_rate: number | null;
  avg_pnl: number | null;
  avg_liquidity: number | null;
  avg_volume24hr: number | null;
}

interface LiquidityData {
  sizeBreakdown: SizeRow[];
  depthBreakdown: DepthRow[];
  marketBreakdown: MarketRow[];
}

function pct(v: number | null) {
  return v !== null ? `${(v * 100).toFixed(1)}%` : "—";
}
function pts(v: number | null) {
  return v !== null ? `${v.toFixed(1)}pt` : "—";
}
function num(v: number | null, digits = 0) {
  return v !== null ? v.toFixed(digits) : "—";
}
function dollar(v: number | null) {
  if (v === null) return "—";
  return v >= 1000 ? `$${(v / 1000).toFixed(1)}k` : `$${v.toFixed(0)}`;
}

export function LiquidityTab({ filters }: { filters: FilterParams }) {
  const [data, setData] = React.useState<LiquidityData | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/liquidity?${filtersToSearchParams(filters)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => {
        setData(d);
        setLoading(false);
      })
      .catch((e) => {
        setError(e.message);
        setLoading(false);
      });
  }, [filters]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12 text-sm text-muted-foreground">
        Loading liquidity data...
      </div>
    );
  }
  if (error || !data) {
    return (
      <div className="text-sm text-destructive py-8">
        Error: {error || "No data"}
      </div>
    );
  }

  const hasSize = data.sizeBreakdown.some((r) => r.bucket !== "Unknown");
  const hasDepth = data.depthBreakdown.some((r) => r.bucket !== "Unknown");
  const hasMarket = data.marketBreakdown.some((r) => r.bucket !== "Unknown");

  return (
    <div className="space-y-6">
      {/* Section 1: Best Ask Size at Entry */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            First-Leg Best Ask Size at Entry
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            Size of the best ask for the token you entered (from WebSocket).
            Thin orders may indicate illiquid conditions.
          </p>
        </CardHeader>
        <CardContent>
          {!hasSize ? (
            <p className="text-sm text-muted-foreground">
              No size data yet — will populate as new attempts are recorded.
            </p>
          ) : (
            <div className="space-y-4">
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={data.sizeBreakdown.filter((r) => r.bucket !== "Unknown")}
                    margin={{ top: 4, right: 16, bottom: 4, left: 0 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="bucket" tick={{ fontSize: 11 }} />
                    <YAxis yAxisId="left" tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} tick={{ fontSize: 11 }} />
                    <YAxis yAxisId="right" orientation="right" tickFormatter={(v) => v.toFixed(1)} tick={{ fontSize: 11 }} />
                    <Tooltip
                      formatter={(value, name) => {
                        if (name === "pair_rate") return [`${((value as number) * 100).toFixed(1)}%`, "Pair Rate"];
                        if (name === "avg_pnl") return [`${(value as number).toFixed(1)}pt`, "Avg PnL"];
                        return [value, name];
                      }}
                    />
                    <ReferenceLine yAxisId="left" y={0} stroke="#666" />
                    <Bar yAxisId="left" dataKey="pair_rate" fill="#6366f1" name="pair_rate" />
                    <Bar yAxisId="right" dataKey="avg_pnl" fill="#22c55e" name="avg_pnl" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Bucket</TableHead>
                    <TableHead className="text-right">Attempts</TableHead>
                    <TableHead className="text-right">Pairs</TableHead>
                    <TableHead className="text-right">Pair Rate</TableHead>
                    <TableHead className="text-right">Avg PnL</TableHead>
                    <TableHead className="text-right">Avg Size</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.sizeBreakdown.map((r) => (
                    <TableRow key={r.bucket}>
                      <TableCell>{r.bucket}</TableCell>
                      <TableCell className="text-right">{r.attempts}</TableCell>
                      <TableCell className="text-right">{r.pairs}</TableCell>
                      <TableCell className="text-right">{pct(r.pair_rate)}</TableCell>
                      <TableCell className="text-right">{pts(r.avg_pnl)}</TableCell>
                      <TableCell className="text-right">{num(r.avg_size, 1)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Section 2: Depth Within 2 Ticks */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            Ask Depth Within 2 Ticks of Best Ask
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            Cumulative ask size within 2 ticks ($0.02) of the best ask at entry.
            Thin depth = single order; deep = real liquidity wall.
          </p>
        </CardHeader>
        <CardContent>
          {!hasDepth ? (
            <p className="text-sm text-muted-foreground">
              No depth data yet — will populate as new attempts are recorded.
            </p>
          ) : (
            <div className="space-y-4">
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={data.depthBreakdown.filter((r) => r.bucket !== "Unknown")}
                    margin={{ top: 4, right: 16, bottom: 4, left: 0 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="bucket" tick={{ fontSize: 11 }} />
                    <YAxis yAxisId="left" tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} tick={{ fontSize: 11 }} />
                    <YAxis yAxisId="right" orientation="right" tickFormatter={(v) => v.toFixed(1)} tick={{ fontSize: 11 }} />
                    <Tooltip
                      formatter={(value, name) => {
                        if (name === "pair_rate") return [`${((value as number) * 100).toFixed(1)}%`, "Pair Rate"];
                        if (name === "avg_pnl") return [`${(value as number).toFixed(1)}pt`, "Avg PnL"];
                        return [value, name];
                      }}
                    />
                    <ReferenceLine yAxisId="left" y={0} stroke="#666" />
                    <Bar yAxisId="left" dataKey="pair_rate" fill="#6366f1" name="pair_rate" />
                    <Bar yAxisId="right" dataKey="avg_pnl" fill="#22c55e" name="avg_pnl" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Bucket</TableHead>
                    <TableHead className="text-right">Attempts</TableHead>
                    <TableHead className="text-right">Pairs</TableHead>
                    <TableHead className="text-right">Pair Rate</TableHead>
                    <TableHead className="text-right">Avg PnL</TableHead>
                    <TableHead className="text-right">Avg Depth</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.depthBreakdown.map((r) => (
                    <TableRow key={r.bucket}>
                      <TableCell>{r.bucket}</TableCell>
                      <TableCell className="text-right">{r.attempts}</TableCell>
                      <TableCell className="text-right">{r.pairs}</TableCell>
                      <TableCell className="text-right">{pct(r.pair_rate)}</TableCell>
                      <TableCell className="text-right">{pts(r.avg_pnl)}</TableCell>
                      <TableCell className="text-right">{num(r.avg_depth, 1)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Section 3: Market-Level Liquidity from Gamma API */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            Market-Level Liquidity (Gamma API)
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            Based on Gamma API liquidity field recorded at market discovery.
            Run the backfill script to populate historical markets.
          </p>
        </CardHeader>
        <CardContent>
          {!hasMarket ? (
            <p className="text-sm text-muted-foreground">
              No market liquidity data yet. New markets will have this populated
              automatically. Run{" "}
              <code className="text-xs bg-muted px-1 rounded">
                scripts/backfill_gamma_metrics.py
              </code>{" "}
              to backfill historical markets.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Liquidity Tier</TableHead>
                  <TableHead className="text-right">Attempts</TableHead>
                  <TableHead className="text-right">Markets</TableHead>
                  <TableHead className="text-right">Pair Rate</TableHead>
                  <TableHead className="text-right">Avg PnL</TableHead>
                  <TableHead className="text-right">Avg Liquidity</TableHead>
                  <TableHead className="text-right">Avg Vol 24h</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.marketBreakdown.map((r) => (
                  <TableRow key={r.bucket}>
                    <TableCell>{r.bucket}</TableCell>
                    <TableCell className="text-right">{r.attempts}</TableCell>
                    <TableCell className="text-right">{r.markets}</TableCell>
                    <TableCell className="text-right">{pct(r.pair_rate)}</TableCell>
                    <TableCell className="text-right">{pts(r.avg_pnl)}</TableCell>
                    <TableCell className="text-right">{dollar(r.avg_liquidity)}</TableCell>
                    <TableCell className="text-right">{dollar(r.avg_volume24hr)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
