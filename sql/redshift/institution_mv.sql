CREATE MATERIALIZED VIEW institution_mv AS
SELECT i.affiliation_id,
       i.display_name,
       i.ror_id          AS ror,
       i.iso3166_code    AS country_code,
       LOWER(r.ror_type) AS type,
       ic.paper_count    AS count,
       ic.oa_paper_count,
       ic.citation_count AS citations
FROM institution i
         LEFT JOIN ror r ON i.ror_id = r.ror_id
         LEFT JOIN citation_institutions_mv ic ON i.affiliation_id = ic.affiliation_id;

alter table institution_mv
    owner to awsuser;