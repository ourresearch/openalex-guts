--- what to update when ROR updates, or when we add manual rors to mid.institution


-- need to update mid.institution using mid.institution and ins.ror_summary_view (a view based on ins.ror* tables)
-- specifically need to update these columns:
--       normalized_name,
--       match_name,
--       display_name,
--       grid_id,
--       official_page,
--       wiki_page,
--       iso3166_code,
--       latitude,
--       longitude,
--       updated_date
-- do it in a way that keeps what was there if there is nothing in ror
-- do a batch overwrite of anything that ror has for this; ror is the source of truth

create table mid.zz_bak_institution as (select * from mid.institution);


update mid.institution set
       match_name=f_matching_string(ror.name),
       display_name=ror.name,
       grid_id=ror.grid_id,
       official_page=ror.official_page,
       wiki_page=ror.wikipedia_url,
       iso3166_code=ror.country_code,
       latitude=ror.latitude,
       longitude=ror.longitude,
       updated_date=sysdate
from mid.institution t1
join ins.ror_summary ror on t1.ror_id=ror.ror_id



--create table temp_lookup_affiliations as
--(
--         select
--         -- count(*)
--         affil.original_affiliation, inst.affiliation_id
--         from mid.affiliation affil
--         join mid.institution inst on affil.original_affiliation ilike '%' || inst.display_name || '%'
--         where affil.original_affiliation is not null and affil.affiliation_id is null
--         and inst.affiliation_id not in (select affiliation_id from mid.institutions_with_names_bad_for_ilookup)
--)
--
--update mid.affiliation set affiliation_id=t2.affiliation_id, updated_date=sysdate
--from mid.affiliation t1
--join temp_lookup_affiliations t2 on t1.original_affiliation=t2.original_affiliation
--where t1.original_affiliation is not null and t1.affiliation_id is null
2,316,458
