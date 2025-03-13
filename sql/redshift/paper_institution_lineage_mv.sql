CREATE MATERIALIZED VIEW paper_institution_lineage_mv
    DISTKEY (paper_id)
    SORTKEY (paper_id, ancestor_id)
AS
-- First get all direct paper-institution affiliations
WITH direct_affiliations AS (
    SELECT 
        a.paper_id,
        a.author_id,
        a.affiliation_id
    FROM affiliation_mv a
    WHERE a.affiliation_id IS NOT NULL
),
-- Add self-references (each institution is its own ancestor)
self_ancestors AS (
    SELECT
        da.paper_id,
        da.author_id,
        da.affiliation_id AS institution_id,
        da.affiliation_id AS ancestor_id
    FROM direct_affiliations da
),
-- Get all ancestor relationships
institution_ancestors AS (
    SELECT
        da.paper_id,
        da.author_id,
        da.affiliation_id AS institution_id,
        ia.ancestor_id
    FROM direct_affiliations da
    JOIN institution_ancestors_mv ia ON da.affiliation_id = ia.institution_id
)
-- Combine direct and ancestor relationships
SELECT paper_id, author_id, institution_id, ancestor_id
FROM self_ancestors
UNION
SELECT paper_id, author_id, institution_id, ancestor_id
FROM institution_ancestors;

ALTER TABLE paper_institution_lineage_mv
    OWNER TO awsuser;