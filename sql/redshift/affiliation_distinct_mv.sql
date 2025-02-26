CREATE MATERIALIZED VIEW affiliation_distinct_mv
            DISTKEY ( paper_id )
            SORTKEY (paper_id, author_sequence_number, affiliation_id)
AS
SELECT *
FROM (SELECT a.paper_id,
             a.author_id,
             a.affiliation_id,
             a.author_sequence_number,
             a.original_author,
             a.original_orcid,
             i.display_name                         AS institution_display_name,
             i.ror,
             i.type,
             c.country_id,
             c.display_name                         AS country_display_name,
             c.continent_id,
             c.is_global_south,
             con.display_name                       AS continent_display_name,
             ROW_NUMBER() OVER (PARTITION BY a.paper_id, a.affiliation_id
                 ORDER BY a.author_sequence_number) AS affiliation_rank
      FROM affiliation a
               LEFT JOIN institution_mv i ON a.affiliation_id = i.affiliation_id
               LEFT JOIN country c ON i.country_code = c.country_id
               LEFT JOIN continent con ON c.continent_id = con.continent_id) AS ranked
WHERE ranked.affiliation_rank = 1;

alter table affiliation_distinct_mv
    owner to awsuser;
