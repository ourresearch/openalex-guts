CREATE MATERIALIZED VIEW paper_type_distinct_mv
            DISTKEY (paper_id)
            SORTKEY (paper_id, type)
AS
SELECT DISTINCT a.paper_id, a.type
FROM affiliation_distinct_mv a
WHERE a.type IS NOT NULL;

alter table paper_type_distinct_mv
    owner to awsuser;