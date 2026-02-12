"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";

interface ProjectionData {
  pair_rate: number;
  avg_profit_points: number;
  exit_loss_points: number;
  breakeven_pair_rate: number;
  ev_per_attempt: number;
  avg_attempts_per_market: number;
  markets_per_day: number;
  attempts_per_day: number;
  daily_ev_points: number;
  monthly_ev_points: number;
  daily_ev_dollars: number;
  monthly_ev_dollars: number;
}

interface ProjectionPanelProps {
  projection: ProjectionData | null;
}

function Row({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent?: boolean;
}) {
  return (
    <div className="flex justify-between text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className={accent ? "font-semibold" : ""}>{value}</span>
    </div>
  );
}

export function ProjectionPanel({ projection }: ProjectionPanelProps) {
  if (!projection) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium">
            Profitability Projection
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-32 flex items-center justify-center text-muted-foreground text-sm">
            No data available
          </div>
        </CardContent>
      </Card>
    );
  }

  const pair_rate = Number(projection.pair_rate) || 0;
  const avg_profit_points = Number(projection.avg_profit_points) || 0;
  const exit_loss_points = Number(projection.exit_loss_points) || 0;
  const breakeven_pair_rate = Number(projection.breakeven_pair_rate) || 0;
  const ev_per_attempt = Number(projection.ev_per_attempt) || 0;
  const avg_attempts_per_market = Number(projection.avg_attempts_per_market) || 0;
  const markets_per_day = Number(projection.markets_per_day) || 0;
  const attempts_per_day = Number(projection.attempts_per_day) || 0;
  const daily_ev_points = Number(projection.daily_ev_points) || 0;
  const monthly_ev_points = Number(projection.monthly_ev_points) || 0;
  const daily_ev_dollars = Number(projection.daily_ev_dollars) || 0;
  const monthly_ev_dollars = Number(projection.monthly_ev_dollars) || 0;

  const profitable = ev_per_attempt > 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium">
          Profitability Projection
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <Row
          label="Observed Pair Rate"
          value={`${(pair_rate * 100).toFixed(1)}%`}
        />
        <Row
          label="Avg Profit per Pair"
          value={`${avg_profit_points.toFixed(1)} pts`}
        />
        <Row
          label="Exit Loss (assumed)"
          value={`${exit_loss_points} pts`}
        />
        <Row
          label="Breakeven Pair Rate"
          value={`${(breakeven_pair_rate * 100).toFixed(1)}%`}
        />
        <Row
          label="EV per Attempt"
          value={`${ev_per_attempt.toFixed(3)} pts ($${(ev_per_attempt / 100).toFixed(5)})`}
          accent
        />

        <Separator />

        <Row
          label="Avg Attempts / Market"
          value={avg_attempts_per_market.toFixed(1)}
        />
        <Row
          label="Markets / Day (4 assets)"
          value={String(markets_per_day)}
        />
        <Row
          label="Attempts / Day"
          value={attempts_per_day.toFixed(0)}
        />

        <Separator />

        <Row
          label="Daily EV"
          value={`${daily_ev_points.toFixed(1)} pts ($${daily_ev_dollars.toFixed(2)})`}
          accent
        />
        <Row
          label="Monthly EV"
          value={`${monthly_ev_points.toFixed(0)} pts ($${monthly_ev_dollars.toFixed(2)})`}
          accent
        />

        <div
          className={`mt-2 rounded-md px-3 py-2 text-center text-sm font-medium ${
            profitable
              ? "bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300"
              : "bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-300"
          }`}
        >
          {profitable
            ? `+EV: Above breakeven by ${((pair_rate - breakeven_pair_rate) * 100).toFixed(1)}pp`
            : `-EV: Below breakeven by ${((breakeven_pair_rate - pair_rate) * 100).toFixed(1)}pp`}
        </div>
      </CardContent>
    </Card>
  );
}
