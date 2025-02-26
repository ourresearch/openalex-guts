CREATE MATERIALIZED VIEW affiliation_mv AS
SELECT a.paper_id,
       a.author_id,
       a.affiliation_id,
       a.author_sequence_number,
       a.original_author,
       a.original_orcid,
       a.is_corresponding_author,
       i.display_name AS institution_display_name,
       i.ror,
       i.type,
       c.country_id,
       c.display_name AS country_display_name,
       c.continent_id,
       c.is_global_south
FROM affiliation a
         LEFT JOIN institution_mv i
                   ON a.affiliation_id = i.affiliation_id
         LEFT JOIN country c
                   ON i.country_code = c.country_id;

alter table affiliation_mv
    owner to awsuser;