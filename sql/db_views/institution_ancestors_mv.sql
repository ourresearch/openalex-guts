CREATE MATERIALIZED VIEW mid.institution_ancestors_mv AS
SELECT midinst.affiliation_id AS institution_id,
       midinst.ror_id as ror_id,
       midinst.display_name as display_name,
       ancestor_inst.affiliation_id AS ancestor_id,
       ancestor_inst.ror_id as ancestor_ror_id,
       ancestor_inst.display_name as ancestor_display_name
FROM mid.institution AS midinst
JOIN ins.ror_relationships AS ins ON midinst.ror_id = ins.ror_id
JOIN mid.institution AS ancestor_inst ON ins.related_ror_id = ancestor_inst.ror_id
WHERE ins.relationship_type = 'Parent';