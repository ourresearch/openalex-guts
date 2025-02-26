CREATE MATERIALIZED VIEW paper_author_distinct_mv AS
SELECT paper_id,
       LISTAGG(DISTINCT author_id, '|') WITHIN GROUP (ORDER BY author_sequence_number) AS author_ids
FROM (SELECT paper_id, author_id, author_sequence_number
      FROM affiliation
      WHERE author_sequence_number <= 5000 -- Only keep the first 5,000 authors per paper
     ) subquery
GROUP BY paper_id;

alter table paper_author_distinct_mv
    owner to awsuser;