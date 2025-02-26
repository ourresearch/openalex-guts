CREATE MATERIALIZED VIEW paper_institution_distinct_mv AS
SELECT paper_id,
       LISTAGG(DISTINCT affiliation_id, '|') WITHIN GROUP (ORDER BY author_sequence_number) AS affiliation_ids
FROM (SELECT paper_id, affiliation_id, author_sequence_number
      FROM affiliation
      WHERE author_sequence_number <= 5000 -- Only keep the first 5,000 authors per paper
     ) subquery
GROUP BY paper_id;

alter table paper_institution_distinct_mv
    owner to awsuser;