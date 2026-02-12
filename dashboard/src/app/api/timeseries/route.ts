import { NextRequest, NextResponse } from "next/server";
import { parseFiltersFromParams } from "@/lib/filters";
import { getTimeSeriesDaily, getTimeSeriesHourly } from "@/lib/queries";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const filters = parseFiltersFromParams(params);

    const [daily, hourly] = await Promise.all([
      getTimeSeriesDaily(filters),
      getTimeSeriesHourly(filters),
    ]);

    return NextResponse.json({
      daily: daily.map((r) => ({
        date: r.date,
        attempts: Number(r.attempts),
        pairs: Number(r.pairs),
        pair_rate: r.pair_rate !== null ? Number(r.pair_rate) : null,
        avg_profit: r.avg_profit !== null ? Number(r.avg_profit) : null,
        total_pnl: Number(r.total_pnl),
      })),
      hourly: hourly.map((r) => ({
        hour: Number(r.hour),
        attempts: Number(r.attempts),
        pairs: Number(r.pairs),
        pair_rate: r.pair_rate !== null ? Number(r.pair_rate) : null,
        avg_profit: r.avg_profit !== null ? Number(r.avg_profit) : null,
        total_pnl: Number(r.total_pnl),
      })),
    });
  } catch (error) {
    console.error("Error fetching timeseries:", error);
    return NextResponse.json(
      { error: "Failed to fetch timeseries" },
      { status: 500 }
    );
  }
}
