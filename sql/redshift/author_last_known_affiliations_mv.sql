CREATE MATERIALIZED VIEW author_last_known_affiliations_mv AS
SELECT a.author_id,
       a.affiliation_id,
       a.institution_display_name,
       a.type,
       w.paper_id,
       w.year,
       ROW_NUMBER() OVER (PARTITION BY a.author_id ORDER BY w.year DESC) AS rank
FROM affiliation_mv a
         JOIN
     work w ON a.paper_id = w.paper_id
WHERE w.year IS NOT NULL
  AND a.affiliation_id IS NOT NULL;

alter table author_last_known_affiliations_mv
    owner to awsuser;