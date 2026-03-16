#!/usr/bin/env python3
"""Run EXPLAIN (ANALYZE, BUFFERS) on the /api/stats query that is timing out.

Mirrors the exact filters from:
  GET /api/stats?deltaPoints=14&s0Points=1&stopLoss=33
                &timeRemainingBucket=12+min,13+min
                &dateAfter=2026-02-10&asset=eth
                &firstLegPriceMin=76&firstLegPriceMax=80

Usage:
    python scripts/explain_stats_query.py
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import load_env_file  # noqa: E402


QUERY = """
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
  COUNT(*)::int                                                                    AS total_attempts,
  SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END)::int              AS total_pairs,
  SUM(CASE WHEN a.status='completed_failed' THEN 1 ELSE 0 END)::int              AS total_failed,
  SUM(CASE WHEN a.fail_reason='stop_loss'   THEN 1 ELSE 0 END)::int              AS total_stopped,
  AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END)               AS pair_rate,
  AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END)     AS avg_ttp,
  AVG(CASE WHEN a.status='completed_paired' THEN a.pair_cost_points   END)       AS avg_cost,
  AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END)       AS avg_pair_profit,
  AVG(
    CASE
      WHEN a.pair_profit_points IS NOT NULL THEN a.pair_profit_points
      WHEN a.status = 'completed_failed' AND a.fail_reason = 'stop_loss'
           THEN -COALESCE(a.stop_loss_threshold_points, a.P1_points)
      WHEN a.status = 'completed_failed' THEN -a.P1_points
    END
  )                                                                               AS avg_profit,
  (SUM(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points ELSE 0 END)
   + SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NOT NULL
              THEN a.pair_profit_points ELSE 0 END)
   - SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NULL
                   AND a.fail_reason = 'stop_loss'
              THEN COALESCE(a.stop_loss_threshold_points, a.P1_points) ELSE 0 END)
   - SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NULL
                   AND COALESCE(a.fail_reason,'') != 'stop_loss'
              THEN a.P1_points ELSE 0 END))::int                                  AS total_pnl,
  COUNT(DISTINCT a.market_id)::int                                                AS num_markets
FROM Attempts a
WHERE a.delta_points                = ANY($1)
  AND a.S0_points                   = ANY($2)
  AND a.stop_loss_threshold_points  = ANY($3)
  AND (
        (a.time_remaining_at_start >= $4 AND a.time_remaining_at_start < $5)
     OR (a.time_remaining_at_start >= $6 AND a.time_remaining_at_start < $7)
      )
  AND a.t1_timestamp               >= $8
  AND a.crypto_asset                = ANY($9)
  AND a.P1_points                  >= $10
  AND a.P1_points                  <= $11
"""

PARAMS = [
    [14],           # $1  delta_points
    [1],            # $2  S0_points
    [33],           # $3  stop_loss_threshold_points
    660,            # $4  time_remaining lo for "12 min"
    720,            # $5  time_remaining hi for "12 min"
    720,            # $6  time_remaining lo for "13 min"
    780,            # $7  time_remaining hi for "13 min"
    "2026-02-10",   # $8  dateAfter
    ["eth"],        # $9  crypto_asset
    76,             # $10 P1_points min
    80,             # $11 P1_points max
]


async def run() -> None:
    import asyncpg

    load_env_file()
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]

    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = 0")
            rows = await conn.fetch(QUERY, *PARAMS)
        print("\n".join(r[0] for r in rows))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run())
