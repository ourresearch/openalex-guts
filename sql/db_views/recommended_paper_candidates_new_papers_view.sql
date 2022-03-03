create view mid.recommended_paper_candidates_new_papers_view as
(
with all_options as (
    select work.paper_id as base_paper_id, 
        concepts_of_related.paper_id as related_paper_id, 
        avg(concepts_of_base.score) as average_base_score, 
        avg(concepts_of_related.score) as average_related_score, 
        count(distinct concepts_of_related.field_of_study) as n
    from mid.work work
    join mid.work_concept_for_api_mv concepts_of_base on work.paper_id=concepts_of_base.paper_id
    join mid.work_concept_for_api_mv concepts_of_related on concepts_of_base.field_of_study=concepts_of_related.field_of_study
    where concepts_of_related.paper_id != work.paper_id
            and work.paper_id > 4200000000
    group by work.paper_id, concepts_of_related.paper_id)
select *,
ROW_NUMBER() over (partition by base_paper_id order by n desc, average_base_score*average_related_score desc) as my_rank, 
average_base_score*average_related_score as combo_score
from all_options
where n >= 3
group by base_paper_id, related_paper_id, n, average_base_score, average_related_score
) with no schema binding;

create table mid.recommended_paper_candidates_new_papers as (select * from mid.recommended_paper_candidates_new_papers_view)