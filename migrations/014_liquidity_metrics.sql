BEGIN;

-- Attempts: bid/ask sizes at entry (from WebSocket, already in memory at trigger time)
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS yes_best_bid_size   REAL;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS yes_best_ask_size   REAL;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS no_best_bid_size    REAL;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS no_best_ask_size    REAL;

-- Attempts: cumulative ask depth within 2 ticks of best ask (from REST POST /books at trigger cycle)
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS yes_ask_depth_2tick REAL;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS no_ask_depth_2tick  REAL;

-- Markets: Gamma API market-level metrics recorded at discovery time
ALTER TABLE Markets  ADD COLUMN IF NOT EXISTS volume24hr          REAL;
ALTER TABLE Markets  ADD COLUMN IF NOT EXISTS liquidity           REAL;
ALTER TABLE Markets  ADD COLUMN IF NOT EXISTS open_interest       REAL;

COMMIT;
