CREATE materialized VIEW mid.citation_concepts_mv distkey(field_of_study_id) sortkey(field_of_study_id) AS (
with
    group_papers as (select field_of_study as field_of_study_id, count(distinct paper_id) as n from mid.work_concept where algorithm_version=2 group by field_of_study),
    group_citations as (select field_of_study as field_of_study_id, count(*) as n from mid.citation cite join mid.work_concept work on work.paper_id = cite.paper_reference_id where algorithm_version=2 group by field_of_study )
(
select
    concept.field_of_study_id,
    coalesce(group_papers.n, 0) as paper_count,
    coalesce(group_citations.n, 0) as citation_count,
    sysdate as updated_date
    from mid.concept_for_api_mv concept
 left outer join group_citations on group_citations.field_of_study_id = concept.field_of_study_id
 left outer join group_papers on group_papers.field_of_study_id = concept.field_of_study_id
)
);