CREATE MATERIALIZED VIEW paper_continent_distinct_mv AS
SELECT *
FROM (SELECT a.paper_id,
             a.author_sequence_number,
             c.continent_id,
             c.is_global_south,
             ROW_NUMBER() OVER (PARTITION BY a.paper_id, c.continent_id
                 ORDER BY a.author_sequence_number) AS continent_rank
      FROM affiliation a
               LEFT JOIN institution_mv i ON a.affiliation_id = i.affiliation_id
               LEFT JOIN country c ON i.country_code = c.country_id) AS ranked
WHERE ranked.continent_rank = 1;

alter table paper_continent_distinct_mv
    owner to awsuser;