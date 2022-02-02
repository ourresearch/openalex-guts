CREATE materialized VIEW mid.citation_authors_mv distkey (author_id) sortkey (author_id) AS (
with
     group_papers as (select author_id, count(distinct paper_id) as n from mid.affiliation group by author_id),
     group_citations as (select author_id, count(*) as n from mid.citation cite join mid.affiliation affil on affil.paper_id = cite.paper_reference_id group by author_id)
(
select
    author.author_id,
    coalesce(group_papers.n, 0) as paper_count,
    coalesce(group_citations.n, 0) as citation_count,
    sysdate as updated_date
    from mid.author author
    left outer join group_citations on group_citations.author_id=author.author_id
    left outer join group_papers on group_papers.author_id = author.author_id
)
);

