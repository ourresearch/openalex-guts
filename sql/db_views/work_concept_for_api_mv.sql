CREATE materialized VIEW mid.work_concept_for_api_mv work_concept_for_api_mv distkey (paper_id) sortkey (paper_id, field_of_study) AS
(select *
from mid.work_concept
where algorithm_version=2
and field_of_study in
(select field_of_study_id from mid.concept where wikidata_id is not null));

