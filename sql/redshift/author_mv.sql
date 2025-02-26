CREATE MATERIALIZED VIEW author_mv AS
WITH
    -- Get the most recent affiliation (i.e. the current institution)
    current_institution AS (SELECT author_id,
                                   affiliation_id,
                                   institution_display_name AS affiliation_display_name,
                                   type
                            FROM author_last_known_affiliations_mv
                            WHERE rank = 1),
    -- Aggregate the top 10 affiliated institutions per author (using the year-based ranking)
    past_institutions AS (SELECT author_id,
                                 LISTAGG(affiliation_id::VARCHAR, '|') WITHIN GROUP (ORDER BY year DESC)  AS past_affiliation_ids,
                                 LISTAGG(institution_display_name, '|') WITHIN GROUP (ORDER BY year DESC) AS past_affiliation_display_names,
                                 LISTAGG(type, '|') WITHIN GROUP (ORDER BY year DESC)                     AS past_institution_types
                          FROM (SELECT DISTINCT author_id,
                                                affiliation_id,
                                                institution_display_name,
                                                type,
                                                year
                                FROM author_last_known_affiliations_mv
                                WHERE rank <= 10) sub
                          GROUP BY author_id)
SELECT a.author_id,
       a.display_name,
       o.orcid,
       CASE WHEN o.orcid IS NOT NULL THEN true ELSE false END AS has_orcid,
       ci.affiliation_id,
       ci.affiliation_display_name,
       ci.type                                                as affiliation_type,
       pi.past_affiliation_ids,
       pi.past_affiliation_display_names,
       pi.past_institution_types
FROM author a
         LEFT JOIN author_orcid o ON a.author_id = o.author_id
         LEFT JOIN current_institution ci ON a.author_id = ci.author_id
         LEFT JOIN past_institutions pi ON a.author_id = pi.author_id;

alter table author_mv
    owner to awsuser;