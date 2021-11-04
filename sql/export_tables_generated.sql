
COMMENT ON table outs.entity_related_entities IS 'DEPRECATED';
COMMENT ON COLUMN outs.entity_related_entities.entity_type IS 'Possible values: af (Affiliation), j (Journal), c (Conference)';
COMMENT ON COLUMN outs.entity_related_entities.related_entity_type IS 'Possible values: af (Affiliation), j (Journal), c (Conference)';
COMMENT ON COLUMN outs.entity_related_entities.related_type IS 'Possible values: 0 (same paper), 1 (common coauthors), 2 (co-cited), 3 (common field of study), 4 (same venue), 5 (A cites B), 6 (B cites A)';
COMMENT ON COLUMN outs.entity_related_entities.score IS 'Confidence range between 0 and 1. Larger number representing higher confidence.';


COMMENT ON table outs.field_of_study_children IS 'Relationship between fields_of_study.';
COMMENT ON COLUMN outs.field_of_study_children.field_of_study_id IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs.field_of_study_children.child_field_of_study_id IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';


COMMENT ON table outs.field_of_study_extended_attributes IS 'Other identifiers for fields_of_study.';
COMMENT ON COLUMN outs.field_of_study_extended_attributes.field_of_study_id IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs.field_of_study_extended_attributes.attribute_type IS 'Possible values: 1 (AUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsarchives04.html#2018AA), 2 (source url), 3 (CUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html)';


COMMENT ON table outs.fields_of_study IS 'Base table for Fields of Study';
COMMENT ON COLUMN outs.fields_of_study.field_of_study_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.fields_of_study.rank IS 'DEPRECATED; no new ranks are being added.';
COMMENT ON COLUMN outs.fields_of_study.level IS 'Possible values: 0-5';
COMMENT ON COLUMN outs.fields_of_study.paper_family_count IS 'DEPRECATED; same value as paper_count.';


COMMENT ON table outs.paper_fields_of_study IS 'Linking table from papers to fields';
COMMENT ON COLUMN outs.paper_fields_of_study.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_fields_of_study.field_of_study_id IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs.paper_fields_of_study.score IS 'Confidence range between 0 and 1. Bigger number representing higher confidence.';
COMMENT ON COLUMN outs.paper_fields_of_study.algorithm_version IS 'NEW; version of algorithm to assign fields. Possible values: 1=old MAG (no longer added), 2=OpenAlex';


COMMENT ON table outs.paper_mesh IS 'MeSH headings assigned to the paper by PubMed';
COMMENT ON COLUMN outs.paper_mesh.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_mesh.descriptor_ui IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs.paper_mesh.descriptor_name IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs.paper_mesh.qualifier_ui IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs.paper_mesh.qualifier_name IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs.paper_mesh.is_major_topc IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';


COMMENT ON table outs.paper_recommendations IS 'Paper recommendations with score';
COMMENT ON COLUMN outs.paper_recommendations.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_recommendations.recommended_paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_recommendations.score IS 'Confidence range between 0 and 1. Bigger number representing higher confidence.';


COMMENT ON table outs.related_field_of_study IS 'Relationships between fields of study';
COMMENT ON COLUMN outs.related_field_of_study.field_of_study_id1 IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs.related_field_of_study.type1 IS 'Possible values: general, disease, disease_cause, medical_treatment, symptom';
COMMENT ON COLUMN outs.related_field_of_study.field_of_study_id2 IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs.related_field_of_study.type2 IS 'Possible values: general, disease, disease_cause, medical_treatment, symptom';
COMMENT ON COLUMN outs.related_field_of_study.rank IS 'DEPRECATED; no new ranks are being added.';


COMMENT ON table outs.affiliations IS 'Base table for affiliations ';
COMMENT ON COLUMN outs.affiliations.affiliation_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.affiliations.rank IS 'DEPRECATED; no new ranks are being added.';
COMMENT ON COLUMN outs.affiliations.grid_id IS 'DEPRECATED; ror_id is the new standard identifier for organizations';
COMMENT ON COLUMN outs.affiliations.ror_id IS 'NEW; ROR for this organization, see https://ror.org, https://ror.org/:ror_id';
COMMENT ON COLUMN outs.affiliations.paper_family_count IS 'DEPRECATED; same value as paper_count.';
COMMENT ON COLUMN outs.affiliations.iso3166_code IS 'Two-letter country codes, see https://en.wikipedia.org/wiki/ISO_3166-2';
COMMENT ON COLUMN outs.affiliations.updated_date IS 'NEW; set values updated from new ror data';


COMMENT ON table outs.author_extended_attributes IS 'Additional author name representations';
COMMENT ON COLUMN outs.author_extended_attributes.author_id IS 'FOREIGN KEY REFERENCES Authors.AuthorId';
COMMENT ON COLUMN outs.author_extended_attributes.attribute_type IS 'Possible values: 1=Alternative name';


COMMENT ON table outs.authors IS 'Base table for affiliations ';
COMMENT ON COLUMN outs.authors.author_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.authors.rank IS 'DEPRECATED; no new ranks are being added';
COMMENT ON COLUMN outs.authors.orcid IS 'NEW; ORCID identifier for this author';
COMMENT ON COLUMN outs.authors.paper_family_count IS 'DEPRECATED; same value as paper_count';
COMMENT ON COLUMN outs.authors.updated_date IS 'NEW; set when changes are made going forward';


COMMENT ON table outs.conference_instances IS 'Base table for Conference Instances';
COMMENT ON COLUMN outs.conference_instances.conference_instance_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.conference_instances.conference_series_id IS 'FOREIGN KEY REFERENCES ConferenceSeries.ConferenceSeriesId';
COMMENT ON COLUMN outs.conference_instances.paper_family_count IS 'DEPRECATED; same value as paper_count';


COMMENT ON table outs.conference_series IS 'Base table for Conference Series';
COMMENT ON COLUMN outs.conference_series.conference_series_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.conference_series.rank IS 'DEPRECATED; no new ranks are being added';
COMMENT ON COLUMN outs.conference_series.paper_family_count IS 'DEPRECATED; same value as paper_count';


COMMENT ON table outs.journals IS 'Base table for Journals';
COMMENT ON COLUMN outs.journals.journal_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.journals.rank IS 'DEPRECATED; no new ranks are being added';
COMMENT ON COLUMN outs.journals.issn IS 'the ISSN-L for the journal (see https://en.wikipedia.org/wiki/International_Standard_Serial_Number#Linking_ISSN)';
COMMENT ON COLUMN outs.journals.issns IS 'NEW; JSON list of all ISSNs for this journal (example: \'["1469-5073","0016-6723"]\' )';
COMMENT ON COLUMN outs.journals.is_oa IS 'NEW; TRUE when the journal is 100% OA';
COMMENT ON COLUMN outs.journals.is_in_doaj IS 'NEW; TRUE when the journal is in DOAJ (see https://doaj.org/)';
COMMENT ON COLUMN outs.journals.paper_family_count IS 'DEPRECATED; same value as paper_count';
COMMENT ON COLUMN outs.journals.updated_date IS 'NEW; set when changes are made going forward';


COMMENT ON table outs.paper_author_affiliations IS 'Links between papers';
COMMENT ON COLUMN outs.paper_author_affiliations.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_author_affiliations.author_id IS 'FOREIGN KEY REFERENCES Authors.AuthorId';
COMMENT ON COLUMN outs.paper_author_affiliations.affiliation_id IS 'FOREIGN KEY REFERENCES Affiliations.AffiliationId';
COMMENT ON COLUMN outs.paper_author_affiliations.author_sequence_number IS '1-based author sequence number. 1: the 1st author listed on paper, 2: the 2nd author listed on paper, etc.';


COMMENT ON table outs.paper_extended_attributes IS 'Extra paper identifiers';
COMMENT ON COLUMN outs.paper_extended_attributes.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_extended_attributes.attribute_type IS 'Possible values: 1=PatentId, 2=PubMedId, 3=PmcId, 4=Alternative Title';


COMMENT ON table outs.paper_references IS 'Paper references ';
COMMENT ON COLUMN outs.paper_references.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_references.paper_reference_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';


COMMENT ON table outs.paper_urls IS 'MeSH headings assigned to the paper by PubMed';
COMMENT ON COLUMN outs.paper_urls.paper_id IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs.paper_urls.source_type IS 'Possible values: 1=Html, 2=Text, 3=Pdf, 4=Doc, 5=Ppt, 6=Xls, 8=Rtf, 12=Xml, 13=Rss, 20=Swf, 27=Ics, 31=Pub, 33=Ods, 34=Odp, 35=Odt, 36=Zip, 40=Mp3, 0/999/NULL=unknown';
COMMENT ON COLUMN outs.paper_urls.url_for_landing_page IS 'NEW; URL for the landing page, when article is free to read';
COMMENT ON COLUMN outs.paper_urls.url_for_pdf IS 'NEW; URL for the PDF, when article is free to read';
COMMENT ON COLUMN outs.paper_urls.host_type IS 'NEW; host type of the free-to-read URL, Possible values: publisher, repository';
COMMENT ON COLUMN outs.paper_urls.version IS 'NEW; version of the free-to-read URL Possible values: submittedVersion, acceptedVersion, publishedVersion';
COMMENT ON COLUMN outs.paper_urls.license IS 'NEW; license of the free-to-read URL (example: cc0, cc-by, publisher-specific)';
COMMENT ON COLUMN outs.paper_urls.repository_institution IS 'NEW; name of repository host of URL';
COMMENT ON COLUMN outs.paper_urls.oai_pmh_id IS 'NEW; OAH-PMH id of the repository record';


COMMENT ON table outs.abstracts_inverted IS 'Inverted abstracts';
COMMENT ON COLUMN outs.abstracts_inverted.paper_id IS 'FOREIGN KEY REFERENCES papers.papers_id';
COMMENT ON COLUMN outs.abstracts_inverted.indexed_abstract IS 'Inverted index, see https://en.wikipedia.org/wiki/Inverted_index';


COMMENT ON table outs.paper_resources IS 'DEPRECATED';
COMMENT ON COLUMN outs.paper_resources.paper_id IS 'FOREIGN KEY REFERENCES papers.papers_id';
COMMENT ON COLUMN outs.paper_resources.resource_type IS 'Bit flags: 1=Project, 2=Data, 4=Code';
COMMENT ON COLUMN outs.paper_resources.resource_url IS 'Url of resource';
COMMENT ON COLUMN outs.paper_resources.source_url IS 'List of urls associated with the project, used to derive resource_url';
COMMENT ON COLUMN outs.paper_resources.relationship_type IS 'Bit flags: 1=Own, 2=Cite';


COMMENT ON table outs.papers IS 'Main data for papers';
COMMENT ON COLUMN outs.papers.paper_id IS 'PRIMARY KEY';
COMMENT ON COLUMN outs.papers.rank IS 'DEPRECATED; no new ranks are being added';
COMMENT ON COLUMN outs.papers.doi IS 'Doi values are upper-cased per DOI standard at https://www.doi.org/doi_handbook/2_Numbering.html#2.4';
COMMENT ON COLUMN outs.papers.doc_type IS 'Possible values: Book, BookChapter, Conference, Dataset, Journal, Patent, Repository, Thesis, NULL : unknown. Patent is DEPRECATED; no new Patents are being added.';
COMMENT ON COLUMN outs.papers.genre IS 'NEW';
COMMENT ON COLUMN outs.papers.is_paratext IS 'NEW';
COMMENT ON COLUMN outs.papers.journal_id IS 'FOREIGN KEY references journals.journal_id';
COMMENT ON COLUMN outs.papers.conference_series_id IS 'DEPRECATED; no longer updated, no new Conference Series are being added. FOREIGN KEY references conference_series.conference_series_id.';
COMMENT ON COLUMN outs.papers.conference_instance_id IS 'DEPRECATED; no longer updated, no new Conference Instances are being added. FOREIGN KEY references conference_instances.conference_instance_id.';
COMMENT ON COLUMN outs.papers.estimated_citation IS 'DEPRECATED; is set equal to citation_count';
COMMENT ON COLUMN outs.papers.family_id IS 'DEPRECATED; no longer updated.';
COMMENT ON COLUMN outs.papers.family_rank IS 'DEPRECATED; no longer updated.';
COMMENT ON COLUMN outs.papers.doc_sub_types IS 'Possible values: "Retracted Publication", "Retraction Notice".';
COMMENT ON COLUMN outs.papers.oa_status IS 'NEW; Possible values: closed, green, gold, hybrid, bronze (see https://en.wikipedia.org/wiki/Open_access#Colour_naming_system)';
COMMENT ON COLUMN outs.papers.best_url IS 'NEW; An url for the paper (see paper_urls table for more)';
COMMENT ON COLUMN outs.papers.best_free_url IS 'NEW; An url of legal free-to-read copy when it exists';
COMMENT ON COLUMN outs.papers.best_free_version IS 'NEW; Possible values: submittedVersion, acceptedVersion, publishedVersion';
COMMENT ON COLUMN outs.papers.doi_lower IS 'NEW; lowercase doi for convenience linking to Unpaywall';
COMMENT ON COLUMN outs.papers.updated_date IS 'NEW; set when changes are made going forward';
COMMENT ON COLUMN outs.papers.best_open_access_url IS 'NEW; The best url for reading this paper for free';












































