CREATE materialized VIEW mid.concept_for_api_mv distkey(field_of_study_id) sortkey(field_of_study_id) AS
(select * from mid.concept where wikidata_id is not null);