import { NextRequest, NextResponse } from "next/server";
import { parseFiltersFromParams } from "@/lib/filters";
import {
  getBreakdown,
  getParameterComparison,
  type BreakdownGroupBy,
} from "@/lib/queries";

export const dynamic = "force-dynamic";

const VALID_GROUP_BY = new Set([
  "delta",
  "s0",
  "stopLoss",
  "asset",
  "timeRemaining",
  "combinedSpread",
  "priceRegime",
  "firstLeg",
  "marketPhase",
  "hourOfDay",
  "dayOfWeek",
  "p1Cost",
]);

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const groupBy = params.get("groupBy") || "delta";

    // Special case: parameter comparison doesn't use filters
    if (groupBy === "parameterSet") {
      const rows = await getParameterComparison();
      return NextResponse.json(
        rows.map((r) => ({
          group_key: `δ${r.delta_points} S0=${r.s0_points}${r.stop_loss_threshold_points ? ` SL=${r.stop_loss_threshold_points}` : ""}`,
          parameter_set_id: r.parameter_set_id,
          name: r.name,
          attempts: Number(r.attempts),
          pairs: Number(r.pairs),
          stopped: Number(r.stopped),
          pair_rate: r.pair_rate !== null ? Number(r.pair_rate) : null,
          avg_ttp: r.avg_ttp !== null ? Number(r.avg_ttp) : null,
          avg_profit: r.avg_profit !== null ? Number(r.avg_profit) : null,
          total_pnl: Number(r.total_pnl),
        }))
      );
    }

    if (!VALID_GROUP_BY.has(groupBy)) {
      return NextResponse.json(
        { error: `Invalid groupBy: ${groupBy}` },
        { status: 400 }
      );
    }

    const filters = parseFiltersFromParams(params);
    const rows = await getBreakdown(groupBy as BreakdownGroupBy, filters);

    return NextResponse.json(
      rows.map((r) => ({
        group_key: r.group_key,
        attempts: Number(r.attempts),
        pairs: Number(r.pairs),
        failed: Number(r.failed),
        stopped: Number(r.stopped),
        pair_rate: r.pair_rate !== null ? Number(r.pair_rate) : null,
        avg_ttp: r.avg_ttp !== null ? Number(r.avg_ttp) : null,
        avg_profit: r.avg_profit !== null ? Number(r.avg_profit) : null,
        total_pnl: Number(r.total_pnl),
        avg_mae: r.avg_mae !== null ? Number(r.avg_mae) : null,
      }))
    );
  } catch (error) {
    console.error("Error fetching breakdown:", error);
    return NextResponse.json(
      { error: "Failed to fetch breakdown" },
      { status: 500 }
    );
  }
}
