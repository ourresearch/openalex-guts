CREATE materialized VIEW mid.citation_institutions_by_year_mv distkey(affiliation_id) sortkey (affiliation_id) AS (
(select affiliation_id, 'paper_count' as type, year, count(distinct affil.paper_id) as n
                        from mid.affiliation affil
                        join mid.work on mid.work.paper_id=affil.paper_id
                        where year <= extract(year from current_date)
                        group by affiliation_id, year)
union
(select affiliation_id, 'citation_count' as type, year, count(*) as n
                        from mid.citation cite
                        join mid.affiliation affil on affil.paper_id = cite.paper_reference_id
                        join mid.work on mid.work.paper_id=cite.paper_id
                        where year <= extract(year from current_date)
                        group by affiliation_id, year)
);
