-- view mid.work_topic_primary_v1_view depends on table mid.work_topic
CREATE OR REPLACE VIEW mid.work_topic_primary_v1_view
AS SELECT DISTINCT ON (work_topic.paper_id) work_topic.paper_id,
    work_topic.topic_id,
    work_topic.score,
    work_topic.algorithm_version,
    work_topic.updated_date
   FROM mid.work_topic
  WHERE work_topic.algorithm_version = 1
  ORDER BY work_topic.paper_id, work_topic.score DESC;

--view mid.citation_topics_view depends on view mid.work_topic_primary_v1_view
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


-- materialized view mid.citation_topics_mv depends on view mid.citation_topics_view
CREATE MATERIALIZED VIEW mid.citation_topics_mv
TABLESPACE pg_default
AS SELECT citation_topics_view.topic_id,
    citation_topics_view.paper_count,
    citation_topics_view.oa_paper_count,
    citation_topics_view.citation_count
   FROM mid.citation_topics_view
WITH DATA;

CREATE UNIQUE INDEX citation_topics_mv_topic_id_idx2 ON mid.citation_topics_mv USING btree (topic_id);

-- view mid.citation_subfields_view depends on materialized view mid.citation_topics_mv
CREATE OR REPLACE VIEW mid.citation_subfields_view
AS SELECT t.subfield_id,
    sum(c.paper_count) AS paper_count,
    sum(c.citation_count) AS citation_count,
    sum(c.oa_paper_count) AS oa_paper_count
   FROM mid.citation_topics_mv c
     JOIN mid.topic t USING (topic_id)
  GROUP BY t.subfield_id;

-- view mid.citation_domains_view depends on materialized view mid.citation_topics_mv
CREATE OR REPLACE VIEW mid.citation_domains_view
AS SELECT t.domain_id,
    sum(c.paper_count) AS paper_count,
    sum(c.citation_count) AS citation_count,
    sum(c.oa_paper_count) AS oa_paper_count
   FROM mid.citation_topics_mv c
     JOIN mid.topic t USING (topic_id)
  GROUP BY t.domain_id;

-- view mid.citation_fields_view depends on materialized view mid.citation_topics_mv
CREATE OR REPLACE VIEW mid.citation_fields_view
AS SELECT t.field_id,
    sum(c.paper_count) AS paper_count,
    sum(c.citation_count) AS citation_count,
    sum(c.oa_paper_count) AS oa_paper_count
   FROM mid.citation_topics_mv c
     JOIN mid.topic t USING (topic_id)
  GROUP BY t.field_id;


-- materialized view mid.citation_subfields_mv depends on view mid.citation_subfields_view
CREATE MATERIALIZED VIEW mid.citation_subfields_mv
TABLESPACE pg_default
AS SELECT citation_subfields_view.subfield_id,
    citation_subfields_view.paper_count,
    citation_subfields_view.oa_paper_count,
    citation_subfields_view.citation_count
   FROM mid.citation_subfields_view
WITH DATA;
CREATE UNIQUE INDEX citation_subfields_mv_subfield_id_idx2 ON mid.citation_subfields_mv USING btree (subfield_id);

-- materialized view mid.citation_fields_mv depends on view mid.citation_fields_view
CREATE MATERIALIZED VIEW mid.citation_fields_mv
TABLESPACE pg_default
AS SELECT citation_fields_view.field_id,
    citation_fields_view.paper_count,
    citation_fields_view.oa_paper_count,
    citation_fields_view.citation_count
   FROM mid.citation_fields_view
WITH DATA;
CREATE UNIQUE INDEX citation_fields_mv_field_id_idx2 ON mid.citation_fields_mv USING btree (field_id);
-- materialized view mid.citation_domains_mv depends on view mid.citation_domains_view
CREATE MATERIALIZED VIEW mid.citation_domains_mv
TABLESPACE pg_default
AS SELECT citation_domains_view.domain_id,
    citation_domains_view.paper_count,
    citation_domains_view.oa_paper_count,
    citation_domains_view.citation_count
   FROM mid.citation_domains_view
WITH DATA;
CREATE UNIQUE INDEX citation_domains_mv_domain_id_idx ON mid.citation_domains_mv USING btree (domain_id);

-- view mid.author_topic_for_api_view depends on table mid.work_topic
CREATE OR REPLACE VIEW mid.author_topic_for_api_view
AS SELECT author.author_id,
    topic.topic_id,
    topic.display_name,
        CASE
            WHEN author.author_id >= '5000000000'::bigint THEN count(DISTINCT affil.paper_id)::numeric
            ELSE 0::numeric
        END AS topic_count
   FROM mid.author author
     JOIN mid.affiliation affil ON affil.author_id = author.author_id
     JOIN ( SELECT t.paper_id,
            t.topic_id,
            t.score_rank
           FROM ( SELECT work_topic.paper_id,
                    work_topic.topic_id,
                    rank() OVER (PARTITION BY work_topic.paper_id ORDER BY work_topic.score DESC) AS score_rank
                   FROM mid.work_topic) t
          WHERE t.score_rank = 1) wtopic ON wtopic.paper_id = affil.paper_id
     JOIN mid.topic topic ON topic.topic_id = wtopic.topic_id
  GROUP BY author.author_id, topic.topic_id, topic.display_name;