CREATE materialized VIEW mid.citation_institutions_mv distkey(affiliation_id) sortkey (affiliation_id) AS (
with
    group_papers as (select affiliation_id, count(distinct paper_id) as n from mid.affiliation group by affiliation_id),
    group_citations as (select affiliation_id, count(*) as n from mid.citation cite join mid.affiliation affil on affil.paper_id = cite.paper_reference_id group by affiliation_id)
(
select
    affil.affiliation_id,
    coalesce(group_papers.n, 0) as paper_count,
    coalesce(group_citations.n, 0) as citation_count,
    sysdate as updated_date
    from mid.institution affil
        left outer join group_citations on group_citations.affiliation_id=affil.affiliation_id
        left outer join group_papers on group_papers.affiliation_id = affil.affiliation_id
)
);