## Authors

model `AuthorCounts` in `counts.py` uses table `mid.citation_authors_mv`

```sql
-- mid.citation_authors_mv source

CREATE MATERIALIZED VIEW mid.citation_authors_mv
TABLESPACE pg_default
AS SELECT citation_authors_view.author_id,
    citation_authors_view.paper_count,
    citation_authors_view.oa_paper_count,
    citation_authors_view.citation_count
   FROM mid.citation_authors_view
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX citation_authors_mv_new_author_id_idx ON mid.citation_authors_mv USING btree (author_id);


-- mid.citation_authors_view source

CREATE OR REPLACE VIEW mid.citation_authors_view
AS SELECT author_papers_mv.author_id,
    count(*) AS paper_count,
    COALESCE(sum(
        CASE
            WHEN work.is_oa THEN 1
            ELSE 0
        END), 0::bigint) AS oa_paper_count,
    COALESCE(sum(c.count), 0::numeric)::bigint AS citation_count
   FROM mid.author_papers_mv
     JOIN mid.live_works_lite_mv work USING (paper_id)
     LEFT JOIN mid.paper_citations_mv c USING (paper_id)
  GROUP BY author_papers_mv.author_id;
```
has columns:
+ "author_id"
+ "paper_count"
+ "oa_paper_count"
+ "citation_count"

refers to:
+ `mid.author_papers_mv`
+ `mid.live_works_lite_mv`
+ `mid.paper_citations_mv`

```sql
-- mid.author_papers_mv source

CREATE MATERIALIZED VIEW mid.author_papers_mv
TABLESPACE pg_default
AS SELECT DISTINCT affiliation.author_id,
    affiliation.paper_id
   FROM mid.affiliation affiliation
  WHERE affiliation.author_id IS NOT NULL AND affiliation.paper_id IS NOT NULL
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX author_papers_mv_new_author_id_paper_id_idx1 ON mid.author_papers_mv USING btree (author_id, paper_id);
```


## Topics

```sql
-- mid.citation_topics_mv source

CREATE MATERIALIZED VIEW mid.citation_topics_mv
TABLESPACE pg_default
AS SELECT citation_topics_view.topic_id,
    citation_topics_view.paper_count,
    citation_topics_view.oa_paper_count,
    citation_topics_view.citation_count
   FROM mid.citation_topics_view
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX citation_topics_mv_topic_id_idx ON mid.citation_topics_mv USING btree (topic_id);


-- mid.citation_authors_view source

CREATE OR REPLACE VIEW mid.citation_topics_view
AS SELECT topic.topic_id,
    count(*) AS paper_count,
    COALESCE(sum(
        CASE
            WHEN work.is_oa THEN 1
            ELSE 0
        END), 0::bigint) AS oa_paper_count,
    COALESCE(sum(c.count), 0::numeric)::bigint AS citation_count
   FROM mid.work_topic_primary_v1_view topic
     JOIN mid.live_works_lite_mv work USING (paper_id)
     LEFT JOIN mid.paper_citations_mv c USING (paper_id)
  GROUP BY topic.topic_id;

-- mid.work_topic_primary_v1_view source

CREATE OR REPLACE VIEW mid.work_topic_primary_v1_view
AS SELECT DISTINCT ON (work_topic.paper_id) work_topic.paper_id,
    work_topic.topic_id,
    work_topic.score,
    work_topic.algorithm_version,
    work_topic.updated_date
   FROM mid.work_topic
  WHERE work_topic.algorithm_version = 1
  ORDER BY work_topic.paper_id, work_topic.score DESC;
```