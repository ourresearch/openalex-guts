 create table ins.recordthresher_record_bak20220122beforenewrecordthresher as (select * from ins.recordthresher_record)
 create table ins.work_match_recordthresher_bak20220122beforenewrecordthresher as (select * from mid.work_match_recordthresher)

select count(*) from ins.recordthresher_record
-- 8,292,279

update ins.recordthresher_record set work_processed_state='has doi matching a work' where doi in (select doi_lower from mid.work) and work_processed_state is null
-- 5,688,415

update ins.recordthresher_record set work_processed_state='has title matching a work' where length(match_title) > 50 and match_title in (select match_title from mid.work) and work_processed_state is null
-- 455,428

update ins.recordthresher_record set work_processed_state='no doi and title too short' where length(match_title) < 50 and doi is null and work_processed_state is null
-- 81,625

update ins.recordthresher_record set work_processed_state='is component' where normalized_type='component' and work_processed_state is null
-- 115,468

update ins.recordthresher_record set work_processed_state='blank match_title' where match_title is null and work_processed_state is null
-- 189,549

select count(*) from ins.recordthresher_record where work_processed_state is null
-- 1,984,461

select count(distinct doi) from ins.recordthresher_record where work_processed_state is null
-- 1,846,623

select count(distinct match_title) from ins.recordthresher_record where work_processed_state is null and doi is null
-- 55,447


select id, count(*) as n from ins.recordthresher_record where work_processed_state is null group by id order by n desc

-- probably not needed
-- DELETE FROM ins.recordthresher_record
-- WHERE id IN
-- (SELECT id
--               FROM (SELECT id,
--                              ROW_NUMBER() OVER (partition BY  id ORDER BY updated desc) AS rnum
--                      FROM ins.recordthresher_record) t
--               WHERE t.rnum > 1);


 -- delete from ins.recordthresher_record where work_processed_state is not null;


 select * from util.max_openalex_id
-- 4210821236

 insert into mid.work
 (paper_id, doi, doi_lower, original_title, match_title, journal_id, doc_type, created_date, updated_date)
 (
 select 4210821236 + 1 + (row_number() over (partition by 1)) as paper_id, doi, doi, max(title) as title, max(match_title) as match_title, max(journal.journal_id) as journal_id, max(normalized_doc_type) as doc_type, sysdate, null
 from ins.recordthresher_record
 left outer join mid.journal journal on journal.issn = journal_issn_l
 where doi is not null and title is not null
 and work_processed_state is null
 group by doi, journal_issn_l
 order by random()
 )


 update ins.recordthresher_record set work_id=t2.paper_id, work_processed_state='made new work'
 from ins.recordthresher_record t1
 join mid.work t2 on t1.doi = t2.doi_lower
 where t1.work_id is null

 select * from util.max_openalex_id

 insert into mid.work
 (paper_id, original_title, match_title, journal_id, doc_type, created_date, updated_date)
 (
 select 4205354939 + 1 + (row_number() over (partition by 1)) as paper_id, max(title) as title, match_title, max(journal.journal_id) as journal_id, max(normalized_doc_type) as doc_type, sysdate, null
 from ins.recordthresher_record
 left outer join mid.journal journal on journal.issn = journal_issn_l
 where (doi is null) and (title is not null) and (length(match_title) > 50)
 group by match_title, journal_issn_l
 order by random()
 )

 update ins.recordthresher_record set work_id=t2.paper_id, work_processed_state='made new work'
 from ins.recordthresher_record t1
 join mid.work t2 on t1.match_title = t2.match_title
 where t1.doi is null and t1.work_id is null


insert into mid.work_match_recordthresher (recordthresher_id, work_id, updated)
(select id, work_id, sysdate from ins.recordthresher_record where work_id is not null
and id not in (select recordthresher_id from mid.work_match_recordthresher))
)


 select work_id is null, count(*) from ins.recordthresher_record group by work_id is null
 FALSE	281621
 TRUE	71713

 delete from ins.recordthresher_record where work_id is null
-- 1336

update ins.recordthresher_record set work_processed_state='made a new work' where work_id is not null and work_processed_state is null





truncate mid.json_works_input

COPY mid.json_works_input
(id, updated, json_save, version, abstract_inverted_index, json_save_with_abstract)
 FROM 's3://openalex-sandbox/json/works-feb8-q4work/'
FORMAT PARQUET
CREDS
-- heather WAIT while it copies over

-- 53012557 q1
-- 52665970 q2
-- 52646888 q3
-- 52672746 q4


select count(distinct id) from mid.json_works_input
-- 204564431

select count(distinct id) from mid.json_works where id in (select id from mid.json_works_input)
-- 204564431
-- 5,485,433
-- 5,480,606
select count(distinct id) from mid.json_works_input where id not in (select id from mid.json_works)
-- 609566

-- 1933.8 seconds
update mid.json_works_input set abstract_inverted_index=t2.indexed_abstract, updated=sysdate
from mid.json_works_input t1
join mid.abstract t2 on t1.id=t2.paper_id
-- 110283394

-- 9587.7 seconds
update mid.json_works_input set json_save_with_abstract=util.f_merge_abstract_index(json_save, abstract_inverted_index), updated=sysdate
-- 210998161

-- delete from mid.json_works where id in (select id from mid.json_works_input)
-- delete from mid.json_works_input where id in (select id from mid.json_works)
or
truncate mid.json_works

truncate mid.json_works;

-- 1116.9 seconds
insert into mid.json_works (id, updated, json_save, version, abstract_inverted_index, json_save_with_abstract)
(select id, max(updated) as updated, max(json_save) as json_save, max(version) as version,
max(abstract_inverted_index) as abstract_inverted_index, max(json_save_with_abstract) as json_save_with_abstract
from json_works_input group by id);
-- 204564431