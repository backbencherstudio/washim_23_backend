-- Add trigram GIN index to avoid btree width limits but keep fast text lookups
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- CONCURRENTLY is not allowed inside Prisma migration transaction; use plain CREATE here
CREATE INDEX apollo_leads_search_trgm
  ON apollo_leads
  USING gin (
    title gin_trgm_ops,
    industry gin_trgm_ops,
    keywords gin_trgm_ops,
    technologies gin_trgm_ops,
    website gin_trgm_ops
  );