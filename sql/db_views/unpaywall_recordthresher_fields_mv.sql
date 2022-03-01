
-- 278.5 seconds
CREATE materialized VIEW ins.unpaywall_recordthresher_fields_mv distkey(recordthresher_id) sortkey(recordthresher_id, doi) AS (
select recordthresher_id as recordthresher_id,
case when length(json_line.doi::text) > 0 then json_line.doi::text else null end as doi,
updated,
json_line.oa_status::text as oa_status,
(json_line.is_paratext::text = 'true') as is_paratext,
json_line.best_oa_location.url::text as best_oa_location_url,
json_line.best_oa_location.version::text as best_oa_location_version,
json_line.best_oa_location.license::text as best_oa_location_license,
json_line.journal_issn_l::text as issn_l,
json_line
from ins.unpaywall_recordthresher_main
);
