CREATE materialized VIEW mid.citation_concepts_by_year_mv distkey(field_of_study_id) sortkey(field_of_study_id) AS (
(select field_of_study as field_of_study_id, 'paper_count' as type, year, count(distinct concept.paper_id) as n
        from mid.work_concept concept
        join mid.work on mid.work.paper_id=concept.paper_id
        where algorithm_version=2 and year <= extract(year from current_date)
        group by field_of_study, year)
union
(select field_of_study as field_of_study_id, 'citation_count' as type, year, count(*) as n
        from mid.citation cite
        join mid.work_concept concept on concept.paper_id = cite.paper_reference_id
        join mid.work on mid.work.paper_id=cite.paper_id
        where algorithm_version=2 and year <= extract(year from current_date)
        group by field_of_study, year)
);
