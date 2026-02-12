import { NextRequest, NextResponse } from "next/server";
import { parseFiltersFromParams } from "@/lib/filters";
import { getOverallStats, getProfitabilityProjection } from "@/lib/queries";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const filters = parseFiltersFromParams(params);

    const [stats, projection] = await Promise.all([
      getOverallStats(filters),
      getProfitabilityProjection(filters),
    ]);

    return NextResponse.json({ stats, projection });
  } catch (error) {
    console.error("Error fetching stats:", error);
    return NextResponse.json(
      { error: "Failed to fetch stats" },
      { status: 500 }
    );
  }
}
