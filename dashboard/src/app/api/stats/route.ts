import { NextRequest, NextResponse } from "next/server";
import { parseFiltersFromParams } from "@/lib/filters";
import { getOverallStats, computeProjection } from "@/lib/queries";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const filters = parseFiltersFromParams(params);

    const stats = await getOverallStats(filters);
    const projection = computeProjection(stats);

    return NextResponse.json({ stats, projection });
  } catch (error) {
    console.error("Error fetching stats:", error);
    return NextResponse.json(
      { error: "Failed to fetch stats" },
      { status: 500 }
    );
  }
}
