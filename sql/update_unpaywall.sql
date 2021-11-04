--- what to update when unpaywall updates


-- specifically need to update these columns in mid.work:

        genre
        is_paratext
        oa_status
        best_url
        best_free_url    best_oa_location -> url
        best_free_version    best_oa_location -> url
        doi_lower,
        updated_date,

-- nothing else in them, so can just override.
-- do doi_lower first so can use it as key


update mid.work set doi_lower=lower(doi)

-- then update everything else

create table mid.zz_bak_work as (select * from mid.work);

-- MAG has better titles though so stick with their titles for now as default.
update mid.work set
        genre=u.genre,
        is_paratext=u.is_paratext,
        oa_status=u.oa_status,
        best_url=u.doi_url,
        best_free_url=json_extract_path_text(u.best_oa_location, 'url'),
        best_free_version=json_extract_path_text(u.best_oa_location, 'version'),
        updated_date=sysdate
from mid.work t1
join ins.unpaywall_raw u on t1.doi_lower=u.doi


-- then update the urls that don't have dois / aren't in unpaywall
-- later make this be a good choice of best url, right now it's just a random one
with best_url as (select paper_id, max(url) as url from mid.location group by paper_id)
update mid.work set
        best_url=loc.url,
        updated_date=sysdate
from mid.work t1
join best_url loc on t1.paper_id=loc.paper_id
where t1.best_url is null



-- then update everything else

create table mid.zz_bak_location as (select * from mid.location);

-- can't do it with an or, so do it twice for each url match
with location_with_paper_id as (select work.paper_id, u.* from ins.unpaywall_oa_location_raw u join mid.work work on u.doi=work.doi_lower)
update mid.location set
    endpoint_id=u.endpoint_id,
    evidence=u.evidence,
    host_type=u.host_type,
    is_best=u.is_best,
    license=u.license,
    oa_date=u.oa_date,
    pmh_id=u.pmh_id,
    repository_institution=u.repository_institution,
    updated=u.updated,
    url=u.url,
    url_for_landing_page=u.url_for_landing_page,
    url_for_pdf=u.url_for_pdf,
    version=u.version
from mid.location t1
join location_with_paper_id u on t1.paper_id=u.paper_id
where lower(replace(t1.source_url, 'https', 'http')) = lower(replace(u.url_for_landing_page, 'https', 'http'));

with location_with_paper_id as (select work.paper_id, u.* from ins.unpaywall_oa_location_raw u join mid.work work on u.doi=work.doi_lower)
update mid.location set
    endpoint_id=u.endpoint_id,
    evidence=u.evidence,
    host_type=u.host_type,
    is_best=u.is_best,
    license=u.license,
    oa_date=u.oa_date,
    pmh_id=u.pmh_id,
    repository_institution=u.repository_institution,
    updated=u.updated,
    url=u.url,
    url_for_landing_page=u.url_for_landing_page,
    url_for_pdf=u.url_for_pdf,
    version=u.version
from mid.location t1
join location_with_paper_id u on t1.paper_id=u.paper_id
where lower(replace(t1.source_url, 'https', 'http')) = lower(replace(u.url_for_pdf, 'https', 'http'));




-- first add anything not there already what we've got
insert into mid.location (
    paper_id,
    source_type,
    language_code,
    source_url,
    endpoint_id,
    evidence,
    host_type,
    is_best,
    license,
    oa_date,
    pmh_id,
    repository_institution,
    updated,
    url,
    url_for_landing_page,
    url_for_pdf,
    version)
     (
        select
            work.paper_id,
            null as source_type,
            null as language_code,
            u.url as source_url,
            u.endpoint_id,
            u.evidence,
            u.host_type,
            u.is_best,
            u.license,
            u.oa_date,
            u.pmh_id,
            u.repository_institution,
            u.updated,
            u.url,
            u.url_for_landing_page,
            u.url_for_pdf,
            u.version
            from ins.unpaywall_oa_location_raw u
            join mid.work work on u.doi=work.doi_lower
            where work.paper_id || lower(replace(u.url, 'https', 'http')) not in
                (select loc.paper_id || lower(replace(loc.source_url, 'https', 'http')) from mid.location loc)
        )




