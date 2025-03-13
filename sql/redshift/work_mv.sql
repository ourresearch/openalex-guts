CREATE MATERIALIZED VIEW work_mv
          DISTKEY (paper_id)
          SORTKEY (year, cited_by_count, original_title)
AS
SELECT w.paper_id,
       w.original_title,
       w.doi,
       w.doi_lower,
       w.journal_id,
       w.publication_date,
       w.is_paratext,
       w.oa_status,
       CASE WHEN w.oa_status <> 'closed' THEN true ELSE false END AS is_oa,
       w.type,
       w.type_crossref,
       w.year,
       w.language,
       w.is_retracted,
       w.license,
       w.created_date,
       w.publisher,
       COALESCE(c.citation_count, 0)                              AS cited_by_count,
       wf.fwci,
       CASE WHEN wfu.work_id IS NOT NULL THEN true ELSE false END AS has_fulltext,
       wt.topic_id,
       t.display_name                                             AS topic_display_name,
       t.domain_id,
       t.field_id,
       t.subfield_id,
       d.display_name                                             AS domain_display_name,
       f.display_name                                             AS field_display_name,
       sf.display_name                                            AS subfield_display_name,
       s.display_name                                             AS primary_source_display_name,
       s.type                                                     AS primary_source_type,
       s.issn                                                     AS primary_source_issn,
       s.is_in_doaj                                               AS primary_source_is_in_doaj,
       aff_auth.author_ids,
       aff_auth.author_display_names,
       aff_auth.institution_ids,
       aff_auth.institution_display_names,
       aff_auth.ror_ids,
       aff_orcid.orcid_ids,
       aff_country.country_ids,
       aff_country.country_display_names,
       aff_continent.continent_ids,
       aff_continent.continent_display_names,
       aff_continent.is_global_south,
       kw.keyword_ids,
       kw.keyword_display_names,
       inst_type.institution_types,
       funder_list.funder_ids,
       funder_list.funder_display_names
FROM work w
     LEFT JOIN citation_papers_mv c ON w.paper_id = c.paper_id
     LEFT JOIN work_fwci wf ON w.paper_id = wf.paper_id
     LEFT JOIN work_topic wt ON w.paper_id = wt.paper_id AND wt.topic_rank = 1
     LEFT JOIN topic t ON wt.topic_id = t.topic_id
     LEFT JOIN domain d ON t.domain_id = d.domain_id
     LEFT JOIN field f ON t.field_id = f.field_id
     LEFT JOIN subfield sf ON t.subfield_id = sf.subfield_id
     LEFT JOIN source s ON w.journal_id = s.journal_id
     
     -- Aggregate funder IDs and display names
     LEFT JOIN (SELECT wfu.paper_id,
                         '|' || LISTAGG(fu.funder_id::VARCHAR, '|') || '|' AS funder_ids,
                         '|' || LISTAGG(fu.display_name, '|') || '|' AS funder_display_names
               FROM work_funder wfu
                         JOIN funder fu ON wfu.funder_id = fu.funder_id
               GROUP BY wfu.paper_id) funder_list ON w.paper_id = funder_list.paper_id
     
     -- Pre-aggregate author, institution, and ROR data with ordering by author_sequence_number
     LEFT JOIN (
     WITH all_affiliations AS (
          -- Get direct affiliations
          SELECT 
               af.paper_id,
               af.author_id,
               af.author_sequence_number,
               a.display_name AS author_display_name,
               af.affiliation_id,
               i.display_name AS institution_display_name,
               af.ror
          FROM affiliation_distinct_mv af
          LEFT JOIN author_mv a ON af.author_id = a.author_id
          LEFT JOIN institution i ON af.affiliation_id = i.affiliation_id
          WHERE af.author_sequence_number <= 5000
     )
     SELECT 
          -- Aggregate all affiliations
          paper_id,
          '|' || LISTAGG(DISTINCT author_id::VARCHAR, '|') WITHIN GROUP (ORDER BY author_sequence_number) || '|' AS author_ids,
          '|' || LISTAGG(DISTINCT author_display_name, '|') WITHIN GROUP (ORDER BY author_sequence_number) || '|' AS author_display_names,
          '|' || LISTAGG(DISTINCT affiliation_id::VARCHAR, '|') || '|' AS institution_ids,
          '|' || LISTAGG(DISTINCT institution_display_name, '|') || '|' AS institution_display_names,
          '|' || LISTAGG(DISTINCT ror, '|') AS ror_ids
     FROM all_affiliations
     GROUP BY paper_id) aff_auth ON w.paper_id = aff_auth.paper_id
     
     -- Pre-aggregate ORCID values (distinct)
     LEFT JOIN (SELECT t1.paper_id,
                         '|' || LISTAGG(t1.orcid, '|') || '|' AS orcid_ids
               FROM (SELECT DISTINCT af.paper_id, a.orcid
                         FROM affiliation_distinct_mv af
                              LEFT JOIN author_mv a
                                        ON af.author_id = a.author_id
                         WHERE af.author_sequence_number <= 10) t1
               GROUP BY t1.paper_id) aff_orcid ON w.paper_id = aff_orcid.paper_id
     
     -- Pre-aggregate country data (distinct)
     LEFT JOIN (SELECT t2.paper_id,
                         '|' || LISTAGG(t2.country_id::VARCHAR, '|') || '|'  AS country_ids,
                         '|' || LISTAGG(t2.country_display_name, '|') || '|' AS country_display_names
               FROM (SELECT DISTINCT af.paper_id,
                                        af.country_id,
                                        af.country_display_name
                         FROM affiliation_distinct_mv af
                         WHERE af.author_sequence_number <= 10) t2
               GROUP BY t2.paper_id) aff_country ON w.paper_id = aff_country.paper_id
     
     -- Pre-aggregate continent data (distinct) and is_global_south flag
     LEFT JOIN (SELECT t3.paper_id,
                         '|' || LISTAGG(t3.continent_id::VARCHAR, '|') || '|'  AS continent_ids,
                         '|' || LISTAGG(t3.continent_display_name, '|') || '|' AS continent_display_names,
                         MAX(CASE WHEN t3.is_global_south THEN 1 ELSE 0 END)::BOOLEAN AS is_global_south
               FROM (SELECT DISTINCT af.paper_id,
                                        af.continent_id,
                                        af.continent_display_name,
                                        af.is_global_south
                         FROM affiliation_distinct_mv af
                         WHERE af.author_sequence_number <= 10) t3
               GROUP BY t3.paper_id) aff_continent ON w.paper_id = aff_continent.paper_id
    
     -- Pre-aggregate institution types (distinct)
     LEFT JOIN (SELECT af.paper_id,
                         '|' || LISTAGG(DISTINCT af."type", '|') WITHIN GROUP (ORDER BY af."type") || '|' AS institution_types
               FROM affiliation_distinct_mv af
               WHERE af.author_sequence_number <= 10
               GROUP BY af.paper_id) inst_type ON w.paper_id = inst_type.paper_id
    
     -- Aggregate all keywords per paper without a window function
     LEFT JOIN (SELECT wk.paper_id,
                         '|' || LISTAGG(wk.keyword_id::VARCHAR, '|')
                         WITHIN GROUP (ORDER BY wk.keyword_id) || '|' AS keyword_ids,
                         '|' || LISTAGG(k.display_name, '|')
                         WITHIN GROUP (ORDER BY wk.keyword_id) || '|' AS keyword_display_names
               FROM work_keyword_concept wk
                         JOIN keyword k
                              ON wk.keyword_id = k.keyword_id
               GROUP BY wk.paper_id) kw ON w.paper_id = kw.paper_id
     LEFT JOIN (SELECT DISTINCT work_id
               FROM work_fulltext) wfu ON wfu.work_id = w.paper_id
WHERE w.merge_into_id IS NULL;

alter table work_mv
    owner to awsuser;