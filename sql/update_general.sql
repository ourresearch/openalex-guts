--- what to update

-- create updated_date in all mid tables that I expect it for in the export
-- set updated_date when I do anything :)

-- crossref and pubmed already done, wrt pulling in orcids.
-- No need to update old things in the future; will be triggered by new records.


update legacy.mag_main_paper_extended_attributes set attribute_value=replace(attribute_value, '\\\\', '') where attribute_value ~ '\\\\';


alter table mid.affiliation rename column normalized_author to match_author;
alter table mid.affiliation rename column normalized_institution_name to match_institution_name;
-- has no normalized columns in mag
update mid.affiliation set match_institution_name = f_matching_string(original_affiliation) where original_affiliation is not null; -- took 1500
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
update mid.institution set normalized_name = f_mag_normalize_string(display_name)

