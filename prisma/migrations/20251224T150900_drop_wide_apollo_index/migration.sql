-- Remove oversized composite btree index that breaks inserts (error 54000: index row size exceeds limit)
DROP INDEX IF EXISTS apollo_leads_title_industry_keywords_technologies_website_c_idx;