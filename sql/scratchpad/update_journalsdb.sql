--- what to update when journalsdb updates

--

-- need to update mid.journal
-- specifically need to update these columns:

--       normalized_name,
--       display_name,
--       issn,                    --- the ISSN-L for the journal (see https://en.wikipedia.org/wiki/International_Standard_Serial_Number#Linking_ISSN)
--       issns,                   --- NEW; JSON list of all ISSNs for this journal (example: '["1469-5073","0016-6723"]' )
--       is_oa,                   --- NEW; TRUE when the journal is 100% OA
--       is_in_doaj,              --- NEW; TRUE when the journal is in DOAJ (see https://doaj.org/)
--       publisher,
--       updated_date              --- NEW; set when changes are made going forward

-- do it in a way that keeps what was there if there is nothing in journalsdb
-- do a batch overwrite. MAG has better titles though so stick with their titles for now as default.



-- first update the journalsdb_flat table
create table mid.journalsdb_flat_new distkey (issn) sortkey (issn) as (select * from mid.journalsdb_flat_view)
alter table mid.journalsdb_flat rename to journalsdb_flat_old;
alter table mid.journalsdb_flat_new rename to journalsdb_flat;
drop table journalsdb_flat_old;

-- then update the journal table

create table mid.zz_bak_journal as (select * from mid.journal);

-- MAG has better titles though so stick with their titles for now as default.
update mid.journal set
       display_name=coalesce(t1.display_name, jdb.title),
       normalized_name=coalesce(t1.normalized_name, f_mag_normalize_string(jdb.title)),
       match_name=coalesce(f_matching_string(t1.normalized_name), f_matching_string(jdb.title)),
       issn=jdb.issn_l,
       issns=jdb.issns_string,
       is_oa=jdb.is_gold_journal,
       is_in_doaj=jdb.is_in_doaj,
       publisher=coalesce(jdb.publisher, t1.publisher),
       updated_date=sysdate
from mid.journal t1
join mid.journalsdb_flat jdb on t1.issn=jdb.issn


