-- ============================================================
-- Rankovi Schema v2 Migration
-- Adds multi-pass statistical columns to results table
-- Run once on existing database: psql -U rankovi -d rankovi -f schema_v2_migration.sql
-- ============================================================

-- Add mention_rate: probability score (0.0-1.0) averaged across passes
ALTER TABLE results ADD COLUMN IF NOT EXISTS mention_rate     NUMERIC(5,4) DEFAULT 0.0;

-- Add passes_run: how many times this prompt was fired this run
ALTER TABLE results ADD COLUMN IF NOT EXISTS passes_run       INT DEFAULT 1;

-- Add passes_mentioned: how many of those passes returned a brand mention
ALTER TABLE results ADD COLUMN IF NOT EXISTS passes_mentioned INT DEFAULT 0;

-- Backfill existing rows (single-pass legacy data)
UPDATE results SET
    mention_rate     = CASE WHEN brand_mentioned THEN 1.0 ELSE 0.0 END,
    passes_run       = 1,
    passes_mentioned = CASE WHEN brand_mentioned THEN 1 ELSE 0 END
WHERE passes_run = 1 AND mention_rate IS NULL;

-- Index for analytics queries on mention_rate
CREATE INDEX IF NOT EXISTS idx_results_mention_rate ON results(mention_rate);

-- Verify
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'results'
  AND column_name IN ('mention_rate','passes_run','passes_mentioned')
ORDER BY column_name;
