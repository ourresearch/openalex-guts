CREATE materialized VIEW mid.citation_journals_by_year_mv AS (
(select journal_id, 'paper_count' as type, year, count(distinct paper_id) as n
from mid.work
where year <= extract(year from current_date)
group by journal_id, year)
union
(select journal_id, 'citation_count' as type, year, count(*) as n
from mid.citation cite
join mid.work work on work.paper_id = cite.paper_id
where year <= extract(year from current_date)
group by journal_id, year)
);