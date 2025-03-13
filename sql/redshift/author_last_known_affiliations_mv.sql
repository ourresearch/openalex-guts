CREATE MATERIALIZED VIEW author_last_known_affiliations_mv AS
WITH latest_affiliations AS (
    SELECT 
        a.author_id,
        a.affiliation_id,
        a.institution_display_name,
        a.type,
        a.country_id,
        MAX(w.year) AS year
    FROM affiliation_mv a
    JOIN work w ON a.paper_id = w.paper_id
    WHERE w.year IS NOT NULL
      AND a.affiliation_id IS NOT NULL
    GROUP BY a.author_id, a.affiliation_id, a.institution_display_name, a.type, a.country_id
    ),
    ranked_affiliations AS (
        SELECT 
            author_id,
            affiliation_id,
            institution_display_name,
            type,
            country_id,
            NULL AS paper_id, -- We no longer have a specific paper_id since we're grouping
            year,
            ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY year DESC) AS rank,
            FALSE AS is_descendant
        FROM latest_affiliations
    ),
    descendant_affiliations AS (
        SELECT DISTINCT
            ra.author_id,
            ia.institution_id AS affiliation_id,
            ia.display_name AS institution_display_name,
            i.type,
            i.country_code AS country_id,
            NULL AS paper_id,
            ra.year,
            ra.rank,
            TRUE AS is_descendant
        FROM ranked_affiliations ra
        JOIN institution_ancestors_mv ia ON ra.affiliation_id = ia.ancestor_id
        LEFT JOIN institution_mv i ON ia.institution_id = i.affiliation_id
    )
SELECT * FROM ranked_affiliations
UNION ALL
SELECT * FROM descendant_affiliations;