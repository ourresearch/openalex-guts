CREATE MATERIALIZED VIEW author_last_known_affiliations_mv AS
WITH latest_affiliations AS (
    SELECT 
        a.author_id,
        a.affiliation_id,
        a.institution_display_name,
        a.type,
        MAX(w.year) AS year
    FROM affiliation_mv a
    JOIN work w ON a.paper_id = w.paper_id
    WHERE w.year IS NOT NULL
      AND a.affiliation_id IS NOT NULL
    GROUP BY a.author_id, a.affiliation_id, a.institution_display_name, a.type
)
SELECT 
    author_id,
    affiliation_id,
    institution_display_name,
    type,
    NULL AS paper_id, -- We no longer have a specific paper_id since we're grouping
    year,
    ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY year DESC) AS rank
FROM latest_affiliations;