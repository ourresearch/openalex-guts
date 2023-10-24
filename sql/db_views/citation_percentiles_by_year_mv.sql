CREATE MATERIALIZED VIEW mid.citation_percentiles_by_year_mv AS

WITH RankedCitations AS (
    SELECT
        w.year,
        m.citation_count,
        PERCENT_RANK() OVER(PARTITION BY w.year ORDER BY m.citation_count) AS percentile
    FROM mid.citation_papers_mv m
    JOIN mid.work w ON m.paper_id = w.paper_id
    WHERE m.citation_count IS NOT NULL
)

SELECT
    year,
    citation_count,
    percentile
FROM RankedCitations
GROUP BY year, citation_count, percentile;