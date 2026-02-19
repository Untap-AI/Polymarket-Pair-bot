import { NextRequest, NextResponse } from "next/server";
import { parseFiltersFromParams } from "@/lib/filters";
import {
  getOptimizerRanking,
  getOptimizerEnvBreakdown,
  type EnvDimension,
} from "@/lib/queries";

export const dynamic = "force-dynamic";

/**
 * GET /api/optimizer
 *   → ranking of (delta, S0, stop_loss) combos by PNL / market
 *
 * GET /api/optimizer?env=spread&delta=4&s0=1&stopLoss=2
 *   → environmental breakdown for a specific combo
 */
export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const filters = parseFiltersFromParams(params);

    const envDim = params.get("env") as EnvDimension | null;

    // ------------------------------------------------------------------
    // Environmental breakdown for a specific combo
    // ------------------------------------------------------------------
    if (envDim) {
      const delta = Number(params.get("delta"));
      const s0 = Number(params.get("s0"));
      const slRaw = params.get("stopLoss");
      const stopLoss = slRaw === "null" || slRaw === null ? null : Number(slRaw);

      if (isNaN(delta) || isNaN(s0)) {
        return NextResponse.json(
          { error: "delta and s0 are required for env breakdown" },
          { status: 400 }
        );
      }

      const validDims: EnvDimension[] = ["spread", "priceRegime", "timeRemaining"];
      if (!validDims.includes(envDim)) {
        return NextResponse.json(
          { error: `Invalid env dimension: ${envDim}` },
          { status: 400 }
        );
      }

      const rows = await getOptimizerEnvBreakdown(
        envDim,
        delta,
        s0,
        stopLoss,
        filters
      );

      return NextResponse.json(
        rows.map((r) => ({
          bucket: r.bucket,
          attempts: Number(r.attempts),
          markets: Number(r.markets),
          avg_pnl: r.avg_pnl !== null ? Number(r.avg_pnl) : null,
          pnl_per_mkt: r.pnl_per_mkt !== null ? Number(r.pnl_per_mkt) : null,
        }))
      );
    }

    // ------------------------------------------------------------------
    // Main ranking table
    // ------------------------------------------------------------------
    const rows = await getOptimizerRanking(filters);

    return NextResponse.json(
      rows.map((r) => ({
        delta_points: r.delta_points != null ? Number(r.delta_points) : null,
        s0_points: r.s0_points != null ? Number(r.s0_points) : null,
        stop_loss: r.stop_loss_threshold_points != null
          ? Number(r.stop_loss_threshold_points)
          : null,
        attempts: Number(r.attempts),
        markets: Number(r.markets),
        att_per_mkt: Number(r.att_per_mkt),
        pairs: Number(r.pairs),
        stopped: Number(r.stopped),
        pair_rate: r.pair_rate !== null ? Number(r.pair_rate) : null,
        avg_pnl: r.avg_pnl !== null ? Number(r.avg_pnl) : null,
        total_pnl: Number(r.total_pnl),
        pnl_per_mkt: Number(r.pnl_per_mkt),
      }))
    );
  } catch (error) {
    console.error("Error in optimizer API:", error);
    return NextResponse.json(
      { error: "Failed to fetch optimizer data" },
      { status: 500 }
    );
  }
}
