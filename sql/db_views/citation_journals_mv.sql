CREATE materialized VIEW mid.citation_journals_mv distkey(journal_id) sortkey(journal_id) AS (
with
    group_papers as (select journal_id, count(distinct paper_id) as n from mid.work group by journal_id),
    group_citations as (select journal_id, count(*) as n from mid.citation cite join mid.work work on work.paper_id = cite.paper_reference_id group by journal_id)
(
select
    journal.journal_id,
    coalesce(group_papers.n, 0) as paper_count,
    coalesce(group_citations.n, 0) as citation_count,
    sysdate as updated_date
    from mid.journal journal
    left outer join group_citations on group_citations.journal_id=journal.journal_id
    left outer join group_papers on group_papers.journal_id = journal.journal_id
)
);
