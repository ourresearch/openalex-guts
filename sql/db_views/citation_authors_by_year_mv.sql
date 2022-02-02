CREATE materialized VIEW mid.citation_authors_by_year_mv distkey (author_id) sortkey (author_id) AS (
(select author_id, 'paper_count' as type, year, count(distinct affil.paper_id) as n
        from mid.affiliation affil
        join mid.work on mid.work.paper_id=affil.paper_id
        where year <= extract(year from current_date)
        group by author_id, year)
union
(select author_id, 'citation_count' as type, year, count(*) as n
        from mid.citation cite
        join mid.affiliation affil on affil.paper_id = cite.paper_reference_id
        join mid.work on mid.work.paper_id=cite.paper_id
        where year <= extract(year from current_date)
        group by author_id, year)
);