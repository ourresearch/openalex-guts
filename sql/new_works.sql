 create table ins.recordthresher_record_bak20220122beforenewrecordthresher as (select * from ins.recordthresher_record)

 insert into mid.work_match_recordthresher
 (select id as recordthresher_id, work_id, updated
 from ins.recordthresher_record
 where id not in (select recordthresher_id from mid.work_match_recordthresher))

select count(*) from ins.recordthresher_record
-- 8,292,279

update ins.recordthresher_record set work_processed_state='has doi matching a work' where doi in (select doi_lower from mid.work)
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



-- probably not needed
-- DELETE FROM ins.recordthresher_record
-- WHERE id IN
-- (SELECT id
--               FROM (SELECT id,
--                              ROW_NUMBER() OVER (partition BY  id ORDER BY updated desc) AS rnum
--                      FROM ins.recordthresher_record) t
--               WHERE t.rnum > 1);


 select count(*) from ins.recordthresher_record
 2,297,333
 select count(*) from ins.recordthresher_record where work_id is not null
 557,340
 delete from ins.recordthresher_record where work_id is not null
 select count(*) from ins.recordthresher_record where doi in (select doi_lower from mid.work)
 1,154,583
 delete from ins.recordthresher_record where doi in (select doi_lower from mid.work)
 select count(*) from ins.recordthresher_record where match_title in (select match_title from mid.work)
 182,272
 delete from ins.recordthresher_record where match_title in (select match_title from mid.work)
 select count(*) from ins.recordthresher_record where doi is null and (match_title is null) or length(match_title) < 20
 49804

 delete from ins.recordthresher_record where doi is null and (match_title is null) or length(match_title) < 20


 select * from util.max_openalex_id

 insert into mid.work
 (paper_id, doi, doi_lower, original_title, match_title, journal_id, doc_type, created_date, updated_date)
 (
 select 4205086888 + 1 + (row_number() over (partition by 1)) as paper_id, doi, doi, max(title) as title, max(match_title) as match_title, max(journal.journal_id) as journal_id, max(normalized_doc_type) as doc_type, sysdate, sysdate
 from ins.recordthresher_record
 left outer join mid.journal journal on journal.issn = journal_issn_l
 where doi is not null and title is not null
 group by doi, journal_issn_l
 order by random()
 )


 update ins.recordthresher_record set work_id=t2.paper_id
 from ins.recordthresher_record t1
 join mid.work t2 on t1.doi = t2.doi_lower
 where t1.work_id is null

 select * from util.max_openalex_id

 insert into mid.work
 (paper_id, original_title, match_title, journal_id, doc_type, created_date, updated_date)
 (
 select 4205354939 + 1 + (row_number() over (partition by 1)) as paper_id, max(title) as title, match_title, max(journal.journal_id) as journal_id, max(normalized_doc_type) as doc_type, sysdate, sysdate
 from ins.recordthresher_record
 left outer join mid.journal journal on journal.issn = journal_issn_l
 where (doi is null) and (title is not null) and (length(match_title) > 50)
 group by match_title, journal_issn_l
 order by random()
 )

 update ins.recordthresher_record set work_id=t2.paper_id
 from ins.recordthresher_record t1
 join mid.work t2 on t1.match_title = t2.match_title
 where t1.doi is null and t1.work_id is null


 select work_id is null, count(*) from ins.recordthresher_record group by work_id is null
 FALSE	281621
 TRUE	71713

 delete from ins.recordthresher_record where work_id is null


