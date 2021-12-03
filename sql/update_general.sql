--- what to update

-- create updated_date in all mid tables that I expect it for in the export
-- set updated_date when I do anything :)

-- crossref and pubmed already done, wrt pulling in orcids.
-- No need to update old things in the future; will be triggered by new records.


update legacy.mag_main_paper_extended_attributes set attribute_value=replace(attribute_value, '\\\\', '') where attribute_value ~ '\\\\';


alter table mid.affiliation rename column normalized_author to match_author;
alter table mid.affiliation rename column normalized_institution_name to match_institution_name;
-- has no normalized columns in mag
update mid.affiliation set match_institution_name = f_matching_string(original_affiliation) where original_affiliation is not null; -- took 1245
update mid.affiliation set match_author = f_matching_author_string(original_author);  -- running now 5336

727,725,796

alter table mid.work add column match_title varchar(65000);
-- normalized name in mag is called paper_title
alter table mid.work drop column normalized_title; -- use match_title instead
update mid.work set match_title = f_matching_string(original_title); -- took 1580 seconds
update mid.work set paper_title = f_mag_normalize_string(original_title);  -- took 840 seconds


alter table mid.author add column match_name varchar(65000);
-- normalized name in mag is called normalized_name
update mid.author set match_name = f_matching_author_string(display_name); -- took 2000 seconds
update mid.author set normalized_name = f_mag_normalize_author_string(display_name);  -- took 700seconds


alter table mid.institution add column match_name varchar(65000);
-- normalized name in mag is called normalized_name
alter table mid.institution drop column normalized_name;
alter table mid.institution rename column mag_normalized_name to normalized_name;
update mid.institution set match_name = f_matching_string(display_name)
--update mid.institution set normalized_name = f_mag_normalize_string(display_name)



create table diff.till_zz20211108_mag_main_journals as (
select journal_id, display_name, issn, publisher, webpage, created_date
from zz20211108_mag_main_journals
minus
select journal_id, display_name, issn, publisher, webpage, created_date
from zz20211025_mag_main_journals
)

create table diff.till_zz20211108_mag_advanced_entity_related_entities as (
select * from zz20211108_mag_advanced_entity_related_entities
minus
select * from zz20211025_mag_advanced_entity_related_entities
)

create table diff.till_zz20211108_mag_advanced_field_of_study_children as (
select * from zz20211108_mag_advanced_field_of_study_children
minus
select * from zz20211025_mag_advanced_field_of_study_children
)

create table diff.till_zz20211108_mag_advanced_field_of_study_extended_attributes as (
select * from zz20211108_mag_advanced_field_of_study_extended_attributes
minus
select * from zz20211025_mag_advanced_field_of_study_extended_attributes
)

create table diff.till_zz20211108_mag_advanced_fields_of_study as (
select field_of_study_id, display_name, main_type, level, create_date from zz20211108_mag_advanced_fields_of_study
minus
select field_of_study_id, display_name, main_type, level, create_date from zz20211025_mag_advanced_fields_of_study
)

create table diff.till_zz20211108_mag_advanced_paper_mesh as (
select * from zz20211108_mag_advanced_paper_mesh
minus
select * from zz20211025_mag_advanced_paper_mesh
)

create table diff.till_zz20211108_mag_advanced_paper_mesh as (
select paper_id, recommended_paper_id from zz20211108_mag_advanced_paper_recommendations
minus
select paper_id, recommended_paper_id from zz20211025_mag_advanced_paper_recommendations

create table diff.till_zz20211108_mag_advanced_related_field_of_study as (
select field_of_study_id1, type1, field_of_study_id2, type2 from zz20211108_mag_advanced_related_field_of_study
minus
select field_of_study_id1, type1, field_of_study_id2, type2 from zz20211025_mag_advanced_related_field_of_study
)

create table diff.till_zz20211108_mag_main_affiliations as (
select affiliation_id, display_name, grid_id, official_page, wiki_page, iso3166_code, latitude, longitude, created_date from zz20211108_mag_main_affiliations
minus
select affiliation_id, display_name, grid_id, official_page, wiki_page, iso3166_code, latitude, longitude, created_date from zz20211025_mag_main_affiliations
)

create table diff.till_zz20211108_mag_main_author_extended_attributes as (
select * from zz20211108_mag_main_author_extended_attributes
minus
select * from zz20211025_mag_main_author_extended_attributes
)

create table diff.till_zz20211108_mag_main_authors as (
select author_id, display_name, last_known_affiliation_id, created_date from zz20211108_mag_main_authors
minus
select author_id, display_name, last_known_affiliation_id, created_date  from zz20211025_mag_main_authors
)

create table diff.till_zz20211108_mag_main_conference_instances as (
select conference_instance_id, display_name, conference_series_id, location from zz20211108_mag_main_conference_instances
minus
select conference_instance_id, display_name, conference_series_id, location from zz20211025_mag_main_conference_instances
)

create table diff.till_zz20211108_mag_main_conference_series as (
select conference_series_id, display_name from zz20211108_mag_main_conference_series
minus
select conference_series_id, display_name from zz20211025_mag_main_conference_series
)

create table diff.till_zz20211108_mag_main_paper_extended_attributes as (
select * from zz20211108_mag_main_paper_extended_attributes
minus
select * from zz20211025_mag_main_paper_extended_attributes
)

create table diff.till_zz20211108_mag_main_paper_resources as (
select * from zz20211108_mag_main_paper_resources
minus
select * from zz20211025_mag_main_paper_resources
)

create table diff.till_zz20211108_mag_main_papers as (
select paper_id, doi, doc_type, paper_title, year, publication_date, publisher, journal_id, doc_sub_types from zz20211108_mag_main_papers
minus
select paper_id, doi, doc_type, paper_title, year, publication_date, publisher, journal_id, doc_sub_types from zz20211025_mag_main_papers
)

create table diff.till_zz20211108_mag_nlp_abstracts_inverted as (
select paper_id from zz20211108_mag_nlp_abstracts_inverted
minus
select paper_id from zz20211025_mag_nlp_abstracts_inverted
)




## different, have bad formatting, look at them
zz20211025_mag_main_journals, like 20, need matching to journalsdb when they have issns
zz20211108_mag_main_affiliations like 10, need matching to rors when they have grids
zz20211108_mag_main_authors like 1.6mil dont bother matching to orcid now
zz20211025_mag_main_paper_urls merge with unpaywall, too many diffs, not sure how to handle this
zz20211025_mag_main_papers about 1,618,704 (about 700k are new, about 50k new ones per day, about 400k of these are dois)

# needs tlc
institution
institution_ror
journal
author
author_orcid
location
work

## just copy over
affiliation
abstract
citation
concept
mesh
work_concept
work_extra_ids

# so many differences, no diff, just overwrite
zz20211025_mag_advanced_paper_fields_of_study
zz20211025_mag_advanced_paper_recommendations
zz20211108_mag_advanced_related_field_of_study
zz20211108_mag_main_paper_author_affiliations

## some new ones, just add, or overwrite is ok
zz20211108_mag_advanced_field_of_study_children
zz20211108_mag_advanced_field_of_study_extended_attributes
zz20211108_mag_advanced_fields_of_study
zz20211025_mag_advanced_paper_fields_of_study
zz20211108_mag_advanced_paper_mesh
zz20211025_mag_main_author_extended_attributes
zz20211108_mag_main_paper_extended_attributes
zz20211108_mag_main_paper_references_id
zz20211025_mag_main_paper_resources
zz20211025_mag_nlp_abstracts_inverted
zz20211025_mag_nlp_paper_citation_contexts


## no changes:
zz20211025_mag_advanced_entity_related_entities
zz20211025_mag_main_conference_instances
zz20211025_mag_main_conference_series
