import { NextRequest, NextResponse } from "next/server";
import { parseFiltersFromParams } from "@/lib/filters";
import {
  getLiquidityBreakdown,
  getDepthBreakdown,
  getMarketLiquidityBreakdown,
} from "@/lib/queries";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const filters = parseFiltersFromParams(request.nextUrl.searchParams);

    const [sizeRows, depthRows, marketRows] = await Promise.all([
      getLiquidityBreakdown(filters),
      getDepthBreakdown(filters),
      getMarketLiquidityBreakdown(filters),
    ]);

    const toNum = (v: unknown) => (v !== null && v !== undefined ? Number(v) : null);

    return NextResponse.json({
      sizeBreakdown: sizeRows.map((r) => ({
        bucket: r.bucket as string,
        attempts: Number(r.attempts),
        pairs: Number(r.pairs),
        pair_rate: toNum(r.pair_rate),
        avg_pnl: toNum(r.avg_pnl),
        avg_size: toNum(r.avg_size),
      })),
      depthBreakdown: depthRows.map((r) => ({
        bucket: r.bucket as string,
        attempts: Number(r.attempts),
        pairs: Number(r.pairs),
        pair_rate: toNum(r.pair_rate),
        avg_pnl: toNum(r.avg_pnl),
        avg_depth: toNum(r.avg_depth),
      })),
      marketBreakdown: marketRows.map((r) => ({
        bucket: r.bucket as string,
        attempts: Number(r.attempts),
        markets: Number(r.markets),
        pair_rate: toNum(r.pair_rate),
        avg_pnl: toNum(r.avg_pnl),
        avg_liquidity: toNum(r.avg_liquidity),
        avg_volume24hr: toNum(r.avg_volume24hr),
      })),
    });
  } catch (error) {
    console.error("Error fetching liquidity data:", error);
    return NextResponse.json(
      { error: "Failed to fetch liquidity data" },
      { status: 500 }
    );
  }
}
