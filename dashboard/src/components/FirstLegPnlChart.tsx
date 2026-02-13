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
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { filtersToSearchParams, type FilterParams } from "@/lib/filters";

interface FirstLegPnlChartProps {
  filters: FilterParams;
}

interface DataPoint {
  p1_cost: number;
  avg_pnl: number;
  attempts: number;
  pairs: number;
}

export function FirstLegPnlChart({ filters }: FirstLegPnlChartProps) {
  const [data, setData] = React.useState<DataPoint[]>([]);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    setLoading(true);
    const qs = filtersToSearchParams(filters);
    fetch(`/api/breakdown?groupBy=p1Cost${qs ? `&${qs}` : ""}`)
      .then((r) => r.json())
      .then((rows: unknown) => {
        const arr = Array.isArray(rows) ? rows : [];
        setData(
          arr
            .filter((r: { group_key?: number | null }) => r.group_key != null)
            .map((r: { group_key: number | null; total_pnl: number; attempts: number; pairs: number }) => {
              const attempts = Number(r.attempts) || 1;
              return {
                p1_cost: Number(r.group_key),
                avg_pnl: Number(r.total_pnl) / attempts,
                attempts,
                pairs: Number(r.pairs),
              };
            })
            .sort((a, b) => a.p1_cost - b.p1_cost)
        );
      })
      .catch((err) => console.error("Failed to fetch first leg PNL:", err))
      .finally(() => setLoading(false));
  }, [filters]);

  if (loading) {
    return (
      <Card>
        <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium">
          Avg P&L per Attempt by First Leg Cost
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">
          Loading...
          </div>
        </CardContent>
      </Card>
    );
  }

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium">
          Avg P&L per Attempt by First Leg Cost
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">
          No data for selected filters
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium">
          Avg P&L per Attempt by First Leg Cost
        </CardTitle>
        <p className="text-xs text-muted-foreground">
          How avg P&L per attempt varies with entry price (¢) of the first leg
        </p>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
            <XAxis
              dataKey="p1_cost"
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => `${v}¢`}
              label={{
                value: "First leg cost (¢)",
                position: "insideBottom",
                offset: -5,
                fontSize: 11,
              }}
            />
            <YAxis
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => `${v.toFixed(1)} pts`}
              label={{
                value: "Avg P&L per attempt (pts)",
                angle: -90,
                position: "insideLeft",
                fontSize: 11,
              }}
            />
            <Tooltip
              formatter={(value: any) => [`${Number(value).toFixed(2)} pts`, "Avg P&L"]}
              labelFormatter={(label: any) => `First leg: ${label}¢`}
              content={({ active, payload }) => {
                if (!active || !payload?.[0]) return null;
                const p = payload[0].payload;
                return (
                  <div className="rounded-md border bg-popover px-3 py-2 text-sm shadow-md">
                    <div className="font-medium">{p.p1_cost}¢</div>
                    <div className="text-muted-foreground">
                      Avg P&L: {p.avg_pnl.toFixed(2)} pts/attempt
                    </div>
                    <div className="text-muted-foreground text-xs">
                      {p.attempts} attempts, {p.pairs} pairs
                    </div>
                  </div>
                );
              }}
            />
            <ReferenceLine y={0} stroke="hsl(var(--muted-foreground))" strokeDasharray="3 3" />
            <Line
              type="monotone"
              dataKey="avg_pnl"
              stroke="#10b981"
              strokeWidth={2}
              dot={{ r: 3 }}
              activeDot={{ r: 5 }}
              name="Avg P&L"
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}
