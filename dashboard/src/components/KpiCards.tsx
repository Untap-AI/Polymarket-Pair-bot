"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

interface StatsData {
  total_attempts: number;
  total_pairs: number;
  total_failed: number;
  total_stopped: number;
  pair_rate: number | null;
  avg_ttp: number | null;
  avg_cost: number | null;
  avg_profit: number | null;
  total_pnl: number;
  num_markets: number;
}

interface ProjectionData {
  pair_rate: number;
  avg_profit_points: number;
  exit_loss_points: number;
  breakeven_pair_rate: number;
  ev_per_attempt: number;
  daily_ev_dollars: number;
  monthly_ev_dollars: number;
}

interface KpiCardsProps {
  stats: StatsData | null;
  projection: ProjectionData | null;
}

function formatPct(val: number | string | null | undefined): string {
  if (val === null || val === undefined) return "-";
  const n = Number(val);
  if (isNaN(n)) return "-";
  return `${(n * 100).toFixed(1)}%`;
}

function formatNum(
  val: number | string | null | undefined,
  decimals = 1,
  suffix = ""
): string {
  if (val === null || val === undefined) return "-";
  const n = Number(val);
  if (isNaN(n)) return "-";
  return `${n.toFixed(decimals)}${suffix}`;
}

function KpiCard({
  title,
  value,
  subtitle,
  accent,
}: {
  title: string;
  value: string;
  subtitle?: string;
  accent?: "green" | "red" | "default";
}) {
  const valueColor =
    accent === "green"
      ? "text-emerald-600 dark:text-emerald-400"
      : accent === "red"
        ? "text-red-600 dark:text-red-400"
        : "";

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-xs font-medium text-muted-foreground">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className={`text-2xl font-bold ${valueColor}`}>{value}</div>
        {subtitle && (
          <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
        )}
      </CardContent>
    </Card>
  );
}

export function KpiCards({ stats, projection }: KpiCardsProps) {
  if (!stats) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        {Array.from({ length: 6 }).map((_, i) => (
          <Card key={i}>
            <CardHeader className="pb-2">
              <div className="h-3 w-20 bg-muted animate-pulse rounded" />
            </CardHeader>
            <CardContent>
              <div className="h-7 w-16 bg-muted animate-pulse rounded" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  const evPositive =
    projection && Number(projection.ev_per_attempt) > 0 ? "green" : "red";

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
      <KpiCard
        title="Total Attempts"
        value={Number(stats.total_attempts).toLocaleString()}
        subtitle={`${Number(stats.num_markets)} markets`}
      />
      <KpiCard
        title="Pair Rate"
        value={formatPct(stats.pair_rate)}
        subtitle={`${Number(stats.total_pairs)} pairs / ${Number(stats.total_failed)} failed`}
      />
      <KpiCard
        title="Avg P&L / Attempt"
        value={formatNum(stats.avg_profit, 1, " pts")}
        subtitle={`Avg pair cost: ${formatNum(stats.avg_cost, 1, " pts")}`}
        accent={Number(stats.avg_profit) >= 0 ? "green" : "red"}
      />
      <KpiCard
        title="Avg Time to Pair"
        value={formatNum(stats.avg_ttp, 1, "s")}
      />
      <KpiCard
        title="EV per Attempt"
        value={
          projection ? formatNum(projection.ev_per_attempt, 3, " pts") : "-"
        }
        subtitle={
          projection
            ? `Breakeven: ${formatPct(projection.breakeven_pair_rate)}`
            : undefined
        }
        accent={projection ? evPositive : "default"}
      />
      <KpiCard
        title="Total P&L"
        value={`${Number(stats.total_pnl)} pts`}
        subtitle={
          projection
            ? `Includes failures (SL or first leg cost). $${formatNum(projection.daily_ev_dollars, 2)}/day proj.`
            : "Includes failures (SL or first leg cost)."
        }
        accent={Number(stats.total_pnl) >= 0 ? "green" : "red"}
      />
    </div>
  );
}
