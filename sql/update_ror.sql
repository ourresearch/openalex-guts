--- what to update when ROR updates, or when we add manual rors to mid.institution_ror


-- need to update mid.institution using mid.institution_ror and ins.ror_summary_view (a view based on ins.ror* tables)
-- specifically need to update these columns:
--       normalized_name,
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

update mid.institution set ror_id=ror.ror_id from mid.institution t1
join mid.institution_ror ror on t1.affiliation_id=ror.institution_id

update mid.institution set
       match_name=f_normalize_string(ror.name),
       display_name=ror.name,
       grid_id=ror.grid_id,
       official_page=ror.official_page,
       wiki_page=ror.wikipedia_url,
       iso3166_code=ror.country_code,
       latitude=ror.latitude,
       longitude=ror.longitude,
       updated_date=sysdate
from mid.institution t1
join ins.ror_summary_view ror on t1.ror_id=ror.ror_id


