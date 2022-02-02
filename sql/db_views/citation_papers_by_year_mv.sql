CREATE materialized VIEW mid.citation_papers_by_year_mv distkey(paper_id) sortkey(paper_id) AS (
(select paper_reference_id as paper_id, 'citation_count' as type, year, count(*) as n
                from mid.citation
                join mid.work on mid.work.paper_id=mid.citation.paper_id
                where year <= extract(year from current_date)
                group by paper_reference_id, year)
);