CREATE VIEW ins.ror_summary_view AS (
with one_link as (select ror_id, max(link) as official_page from ins.ror_links group by ror_id),
     one_type as (select ror_id, max(type) as ror_type from ins.ror_types group by ror_id)
SELECT ror_base.ror_id,
ror_base.name,
one_link.official_page,
ror_institutes.wikipedia_url,
ror_grid_equivalents.grid_id,
ror_addresses.lat as latitude,
ror_addresses.lng as longitude,
ror_base.city,
ror_base.state,
ror_base.country,
ror_grid_equivalents.country_code,
one_type.ror_type
FROM ins.ror_base
left outer JOIN one_link ON ror_base.ror_id = one_link.ror_id
left outer JOIN one_type ON ror_base.ror_id = one_type.ror_id
left outer JOIN ins.ror_grid_equivalents ON ror_base.ror_id::text = ror_grid_equivalents.ror_id::text
left outer JOIN ins.ror_institutes ON ror_base.ror_id::text = ror_institutes.ror_id::text
left outer JOIN ins.ror_addresses ON ror_base.ror_id::text = ror_addresses.ror_id::text
) with no schema binding;
