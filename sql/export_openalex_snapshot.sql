
-- export

-- NEED TO DELETE EVERYTHING IN THESE DIRECTORIES FIRST, THE CLEANPATH DOESN'T DO IT

-- 900 seconds
unload ('SELECT max(trunc(updated)) as updated_date, max(json_save_with_abstract) as json_save FROM mid.json_works where json_save_with_abstract is not null group by id order by updated_date desc')
TO 's3://openalex/data/works/'
CREDS
CLEANPATH
MAXFILESIZE AS 2GB
GZIP
PARTITION BY (updated_date)
MANIFEST VERBOSE;


unload ('SELECT max(trunc(updated)) as updated_date, max(json_save) as json_save FROM mid.json_concepts where json_save is not null group by id order by updated_date desc')
TO 's3://openalex/data/concepts/'
CREDS
CLEANPATH
MAXFILESIZE AS 2GB
GZIP
PARTITION BY (updated_date)
MANIFEST VERBOSE;


unload ('SELECT max(trunc(updated)) as updated_date, max(json_save) as json_save FROM mid.json_institutions where json_save is not null group by id order by updated_date desc')
TO 's3://openalex/data/institutions/'
CREDS
CLEANPATH
MAXFILESIZE AS 2GB
GZIP
PARTITION BY (updated_date)
MANIFEST VERBOSE;


unload ('SELECT max(trunc(updated)) as updated_date, max(json_save) as json_save FROM mid.json_venues where json_save is not null group by id order by updated_date desc')
TO 's3://openalex/data/venues/'
CREDS
CLEANPATH
MAXFILESIZE AS 2GB
GZIP
PARTITION BY (updated_date)
MANIFEST VERBOSE;


unload ('SELECT max(trunc(updated)) as updated_date, max(json_save) as json_save FROM mid.json_authors where json_save is not null group by id order by updated_date desc')
TO 's3://openalex/data/authors/'
CREDS
CLEANPATH
MAXFILESIZE AS 2GB
GZIP
PARTITION BY (updated_date)
MANIFEST VERBOSE;


-- copy in, to test
-- takes about xxx seconds
COPY test.test_json_works_super
 FROM 's3://openalex-sandbox/test-jan24/works/manifest'
CREDS
manifest
delimiter '\t'
ignoreheader 1
-- maxerror 100
GZIP;

create table test.test_json_works_super (json_save_super super);
select count(*) from test.test_json_works_super;
select max(json_save_super.id) from test.test_json_works_super;

-- or if load into a text column, check json parsing like this
-- select max(json_extract_path_text(json_save, 'id')) from test.test_json_works



create table mid.json_works_jan19_input (like mid.json_works);

truncate mid.json_works_jan19_input

COPY mid.json_works_jan19_input
 FROM 's3://openalex-sandbox/json/works-jan19-q1work/'
FORMAT PARQUET
CREDS
-- heather WAIT while it copies over

-- 56490892 q2
-- 57377518 q3
-- 57147121 q4

select count(distinct id) from mid.json_works_input
-- 205173997

select count(distinct id) from mid.json_works where id in (select id from mid.json_works_input)
-- 204564431
-- 5,485,433
-- 5,480,606
select count(distinct id) from mid.json_works_input where id not in (select id from mid.json_works)
-- 609566

delete from mid.json_works where id in (select id from mid.json_works_input)
-- delete from mid.json_works_input where id in (select id from mid.json_works)

insert into mid.json_works (select * from json_works_input)
-- 56490892
-- 57377518
-- 655400


truncate mid.json_authors_input;

COPY mid.json_authors_input
 FROM 's3://openalex-sandbox/json/authors-jan2/'
FORMAT PARQUET
CREDS
-- heather WAIT while it copies over

-- 105138540  7
-- 140766763  9

select count(distinct id) from mid.json_authors_input
208251763

select count(*) from mid.json_authors where id in (select id from mid.json_authors_input)
-- 35,545,316
select count(distinct id) from mid.json_authors_input where id not in (select id from mid.json_authors)
1,352,773

-- delete from mid.json_authors where id in (select id from mid.json_authors_input)
delete from mid.json_authors_input where id in (select id from mid.json_authors)

insert into mid.json_authors (select * from json_authors_input)
-- 105138540
-- 140766763
-- 1,621,443