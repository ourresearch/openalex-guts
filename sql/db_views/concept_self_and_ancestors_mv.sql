CREATE materialized VIEW mid.concept_self_and_ancestors_mv distkey(field_of_study_id) sortkey(field_of_study_id) AS
(select * from mid.concept_ancestor
union
select field_of_study_id as id, display_name as name, level, field_of_study_id as ancestor_id, display_name as ancestor_name, level as ancestor_level
from mid.concept_for_api_mv
);