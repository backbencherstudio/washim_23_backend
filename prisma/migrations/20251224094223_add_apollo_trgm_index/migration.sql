-- Ensure trigram search index exists (idempotent)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS apollo_leads_search_trgm
	ON apollo_leads
	USING gin (
		title gin_trgm_ops,
		industry gin_trgm_ops,
		keywords gin_trgm_ops,
		technologies gin_trgm_ops,
		website gin_trgm_ops
	);
