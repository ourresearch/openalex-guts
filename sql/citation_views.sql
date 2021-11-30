

create materialized view mid.citation_papers_mv as (
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


create materialized view mid.citation_authors_mv as (
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


create materialized view mid.citation_journals_mv as (
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

create materialized view mid.citation_institutions_mv as (
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

create materialized view mid.citation_concepts_mv as (
with
    group_papers as (select field_of_study as field_of_study_id, count(distinct paper_id) as n from mid.work_concept group by field_of_study),
    group_citations as (select field_of_study as field_of_study_id, count(*) as n from mid.citation cite join mid.work_concept work on work.paper_id = cite.paper_reference_id group by field_of_study)
(
select
    concept.field_of_study_id,
    coalesce(group_papers.n, 0) as paper_count,
    coalesce(group_citations.n, 0) as citation_count,
    sysdate as updated_date
    from mid.concept concept
 left outer join group_citations on group_citations.field_of_study_id = concept.field_of_study_id
 left outer join group_papers on group_papers.field_of_study_id = concept.field_of_study_id
)
);






update mid.work set reference_count=v.reference_count, citation_count=v.citation_count, estimated_citation=v.estimated_citation
from mid.work t1
join mid.citation_papers_mv v on t1.paper_id=v.paper_id;

update mid.author set paper_count=v.paper_count, citation_count=v.citation_count
from mid.author t1
join mid.citation_authors_mv v on t1.author_id=v.author_id;

update mid.journal set paper_count=v.paper_count, citation_count=v.citation_count
from mid.journal t1
join mid.citation_journals_mv v on t1.journal_id=v.journal_id;

update mid.institution set paper_count=v.paper_count, citation_count=v.citation_count
from mid.institution t1
join mid.citation_institutions_mv v on t1.affiliation_id=v.affiliation_id;

update mid.concept set paper_count=v.paper_count, citation_count=v.citation_count
from mid.concept t1
join mid.citation_concepts_mv v on t1.field_of_study_id=v.field_of_study_id;

