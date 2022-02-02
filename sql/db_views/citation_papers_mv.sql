CREATE materialized VIEW mid.citation_papers_mv distkey(paper_id) sortkey(paper_id) AS (
with
        reference_count as (select paper_id as citing_paper_id, count(*) as n from mid.citation group by paper_id),
        citation_count as (select paper_reference_id as cited_paper_id, count(*) as n from mid.citation group by paper_reference_id)
(
select
    paper_id,
    coalesce(reference_count.n, 0) as reference_count,
    coalesce(citation_count.n, 0) as citation_count,
    coalesce(citation_count.n, util.f_estimated_citation(citation_count.n, publication_date, publisher)) as estimated_citation,
    sysdate as updated_date
 from mid.work work
 left outer join reference_count on reference_count.citing_paper_id = work.paper_id
 left outer join citation_count on citation_count.cited_paper_id = work.paper_id
)
);

