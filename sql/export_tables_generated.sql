
set enable_case_sensitive_identifier=true;
COMMENT ON TABLE outs."EntityRelatedEntities" IS 'Relationship between papers, authors, fields of study. (advanced/EntityRelatedEntities.txt)';
COMMENT ON COLUMN outs."EntityRelatedEntities"."EntityType" IS 'Possible values: af (Affiliation), j (Journal), c (Conference)';
COMMENT ON COLUMN outs."EntityRelatedEntities"."RelatedEntityType" IS 'Possible values: af (Affiliation), j (Journal), c (Conference)';
COMMENT ON COLUMN outs."EntityRelatedEntities"."RelatedType" IS 'Possible values: 0 (same paper), 1 (common coauthors), 2 (co-cited), 3 (common field of study), 4 (same venue), 5 (A cites B), 6 (B cites A)';
COMMENT ON COLUMN outs."EntityRelatedEntities"."Score" IS 'Confidence range between 0 and 1. Larger number representing higher confidence.';


COMMENT ON TABLE outs."FieldOfStudyChildren" IS 'Relationship between Fields of Study (advanced/FieldOfStudyChildren.txt)';
COMMENT ON COLUMN outs."FieldOfStudyChildren"."FieldOfStudyId" IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs."FieldOfStudyChildren"."ChildFieldOfStudyId" IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';


COMMENT ON TABLE outs."FieldOfStudyExtendedAttributes" IS 'Other identifiers for Fields of Study (advanced/FieldOfStudyExtendedAttributes.txt)';
COMMENT ON COLUMN outs."FieldOfStudyExtendedAttributes"."FieldOfStudyId" IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs."FieldOfStudyExtendedAttributes"."AttributeType" IS 'Possible values: 1 (AUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsarchives04.html#2018AA), 2 (source url), 3 (CUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html)';


COMMENT ON TABLE outs."FieldsOfStudy" IS 'Base table for Fields of Study (advanced/FieldsOfStudy.txt)';
COMMENT ON COLUMN outs."FieldsOfStudy"."FieldOfStudyId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."FieldsOfStudy"."Rank" IS 'FROZEN; no new ranks are being added.';
COMMENT ON COLUMN outs."FieldsOfStudy"."NormalizedName" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."FieldsOfStudy"."Level" IS 'Possible values: 0-5';
COMMENT ON COLUMN outs."FieldsOfStudy"."PaperFamilyCount" IS 'FROZEN; same value as "paper_count.';


COMMENT ON TABLE outs."PaperFieldsOfStudy" IS 'Linking table from papers to fields, with score (advanced/PaperFieldsOfStudy.txt)';
COMMENT ON COLUMN outs."PaperFieldsOfStudy"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperFieldsOfStudy"."FieldOfStudyId" IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs."PaperFieldsOfStudy"."Score" IS 'Confidence range between 0 and 1. Bigger number representing higher confidence.';
COMMENT ON COLUMN outs."PaperFieldsOfStudy"."AlgorithmVersion" IS 'NEW; version of algorithm to assign fields. Possible values: 1=old MAG (FROZEN), 2=OpenAlex';


COMMENT ON TABLE outs."PaperMeSH" IS 'MeSH headings assigned to the paper by PubMed (advanced/PaperMeSH.txt)';
COMMENT ON COLUMN outs."PaperMeSH"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperMeSH"."DescriptorUI" IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs."PaperMeSH"."DescriptorName" IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs."PaperMeSH"."QualifierUI" IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs."PaperMeSH"."QualifierName" IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';
COMMENT ON COLUMN outs."PaperMeSH"."IsMajorTopic" IS 'see https://en.wikipedia.org/wiki/Medical_Subject_Headings';


COMMENT ON TABLE outs."PaperRecommendations" IS 'Paper recommendations with score (advanced/PaperRecommendations.txt)';
COMMENT ON COLUMN outs."PaperRecommendations"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperRecommendations"."RecommendedPaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperRecommendations"."Score" IS 'Confidence range between 0 and 1. Bigger number representing higher confidence.';


COMMENT ON TABLE outs."RelatedFieldOfStudy" IS 'Relationships between fields of study (advanced/RelatedFieldOfStudy.txt)';
COMMENT ON COLUMN outs."RelatedFieldOfStudy"."FieldOfStudyId1" IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs."RelatedFieldOfStudy"."Type1" IS 'Possible values: general, disease, disease_cause, medical_treatment, symptom';
COMMENT ON COLUMN outs."RelatedFieldOfStudy"."FieldOfStudyId2" IS 'FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId';
COMMENT ON COLUMN outs."RelatedFieldOfStudy"."Type2" IS 'Possible values: general, disease, disease_cause, medical_treatment, symptom';
COMMENT ON COLUMN outs."RelatedFieldOfStudy"."Rank" IS 'FROZEN; no new ranks are being added.';


COMMENT ON TABLE outs."Affiliations" IS 'Base table for affiliations/institutions (mag/Affiliations.txt)';
COMMENT ON COLUMN outs."Affiliations"."AffiliationId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."Affiliations"."Rank" IS 'FROZEN; no new ranks are being added.';
COMMENT ON COLUMN outs."Affiliations"."NormalizedName" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."Affiliations"."GridId" IS 'FROZEN; ror_id is the new standard identifier for organizations';
COMMENT ON COLUMN outs."Affiliations"."RorId" IS 'NEW; ROR for this organization, see https://ror.org, https://ror.org/:ror_id';
COMMENT ON COLUMN outs."Affiliations"."PaperFamilyCount" IS 'FROZEN; same value as "paper_count.';
COMMENT ON COLUMN outs."Affiliations"."Iso3166Code" IS 'Two-letter country codes, see https://en.wikipedia.org/wiki/ISO_3166-2';
COMMENT ON COLUMN outs."Affiliations"."UpdatedDate" IS 'NEW; set values updated from new ror data';


COMMENT ON TABLE outs."AuthorExtendedAttributes" IS 'Additional author name representations (mag/AuthorExtendedAttributes.txt)';
COMMENT ON COLUMN outs."AuthorExtendedAttributes"."AuthorId" IS 'FOREIGN KEY REFERENCES Authors.AuthorId';
COMMENT ON COLUMN outs."AuthorExtendedAttributes"."AttributeType" IS 'Possible values: 1=Alternative name';


COMMENT ON TABLE outs."Authors" IS 'Base table for authors (mag/Authors.txt)';
COMMENT ON COLUMN outs."Authors"."AuthorId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."Authors"."Rank" IS 'FROZEN; no new ranks are being added';
COMMENT ON COLUMN outs."Authors"."NormalizedName" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."Authors"."Orcid" IS 'NEW; ORCID identifier for this author';
COMMENT ON COLUMN outs."Authors"."PaperFamilyCount" IS 'FROZEN; same value as "paper_count';
COMMENT ON COLUMN outs."Authors"."UpdatedDate" IS 'NEW; set when changes are made going forward';


COMMENT ON TABLE outs."ConferenceInstances" IS 'FROZEN; Base table for Conference Instances (mag/ConferenceInstances.txt)';
COMMENT ON COLUMN outs."ConferenceInstances"."ConferenceInstanceId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."ConferenceInstances"."NormalizedName" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."ConferenceInstances"."ConferenceSeriesId" IS 'FOREIGN KEY REFERENCES ConferenceSeries.ConferenceSeriesId';
COMMENT ON COLUMN outs."ConferenceInstances"."PaperFamilyCount" IS 'FROZEN; same value as "paper_count';


COMMENT ON TABLE outs."ConferenceSeries" IS 'FROZEN; Base table for Conference Series (mag/ConferenceSeries.txt)';
COMMENT ON COLUMN outs."ConferenceSeries"."ConferenceSeriesId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."ConferenceSeries"."Rank" IS 'FROZEN; no new ranks are being added';
COMMENT ON COLUMN outs."ConferenceSeries"."NormalizedName" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."ConferenceSeries"."PaperFamilyCount" IS 'FROZEN; same value as "paper_count';


COMMENT ON TABLE outs."Journals" IS 'Base table for Journals (mag/Journals.txt)';
COMMENT ON COLUMN outs."Journals"."JournalId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."Journals"."Rank" IS 'FROZEN; no new ranks are being added';
COMMENT ON COLUMN outs."Journals"."NormalizedName" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."Journals"."Issn" IS 'UPDATED; the ISSN-L for the journal (see https://en.wikipedia.org/wiki/International_Standard_Serial_Number#Linking_ISSN)';
COMMENT ON COLUMN outs."Journals"."Issns" IS 'NEW; JSON list of all ISSNs for this journal (example: \'["1469-5073","0016-6723"]\' )';
COMMENT ON COLUMN outs."Journals"."IsOa" IS 'NEW; TRUE when the journal is 100% OA';
COMMENT ON COLUMN outs."Journals"."IsInDoaj" IS 'NEW; TRUE when the journal is in DOAJ (see https://doaj.org/)';
COMMENT ON COLUMN outs."Journals"."PaperFamilyCount" IS 'FROZEN; same value as "paper_count';
COMMENT ON COLUMN outs."Journals"."UpdatedDate" IS 'NEW; set when changes are made going forward';


COMMENT ON TABLE outs."PaperAuthorAffiliations" IS 'Links between papers, authors, and affiliations/institutions. NOTE: It is possible to have multiple rows with same (PaperId, AuthorId, AffiliationId) when an author is associated with multiple affiliations. (mag/PaperAuthorAffiliations.txt)';
COMMENT ON COLUMN outs."PaperAuthorAffiliations"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperAuthorAffiliations"."AuthorId" IS 'FOREIGN KEY REFERENCES Authors.AuthorId';
COMMENT ON COLUMN outs."PaperAuthorAffiliations"."AffiliationId" IS 'FOREIGN KEY REFERENCES Affiliations.AffiliationId';
COMMENT ON COLUMN outs."PaperAuthorAffiliations"."AuthorSequenceNumber" IS '1-based author sequence number. 1: the 1st author listed on paper, 2: the 2nd author listed on paper, etc.';


COMMENT ON TABLE outs."PaperExtendedAttributes" IS 'Extra paper identifiers (mag/PaperExtendedAttributes.txt)';
COMMENT ON COLUMN outs."PaperExtendedAttributes"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperExtendedAttributes"."AttributeType" IS 'Possible values: 1=PatentId, 2=PubMedId, 3=PmcId, 4=Alternative Title';


COMMENT ON TABLE outs."PaperReferences" IS 'Paper references and, in reverse, citations (mag/PaperReferences.txt)';
COMMENT ON COLUMN outs."PaperReferences"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperReferences"."PaperReferenceId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';


COMMENT ON TABLE outs."PaperUrls" IS 'Urls for the paper (mag/PaperUrls.txt)';
COMMENT ON COLUMN outs."PaperUrls"."PaperId" IS 'FOREIGN KEY REFERENCES Papers.PaperId';
COMMENT ON COLUMN outs."PaperUrls"."SourceType" IS 'Possible values: 1=Html, 2=Text, 3=Pdf, 4=Doc, 5=Ppt, 6=Xls, 8=Rtf, 12=Xml, 13=Rss, 20=Swf, 27=Ics, 31=Pub, 33=Ods, 34=Odp, 35=Odt, 36=Zip, 40=Mp3, 0/999/NULL=unknown';
COMMENT ON COLUMN outs."PaperUrls"."UrlForLandingPage" IS 'NEW; URL for the landing page, when article is free to read';
COMMENT ON COLUMN outs."PaperUrls"."UrlForPdf" IS 'NEW; URL for the PDF, when article is free to read';
COMMENT ON COLUMN outs."PaperUrls"."HostType" IS 'NEW; host type of the free-to-read URL, Possible values: publisher, repository';
COMMENT ON COLUMN outs."PaperUrls"."Version" IS 'NEW; version of the free-to-read URL Possible values: submittedVersion, acceptedVersion, publishedVersion';
COMMENT ON COLUMN outs."PaperUrls"."License" IS 'NEW; license of the free-to-read URL (example: cc0, cc-by, publisher-specific)';
COMMENT ON COLUMN outs."PaperUrls"."RepositoryInstitution" IS 'NEW; name of repository host of URL';
COMMENT ON COLUMN outs."PaperUrls"."OaiPmhId" IS 'NEW; OAH-PMH id of the repository record';


COMMENT ON TABLE outs."PaperAbstractsInvertedIndex" IS 'Inverted abstracts (nlp/PaperAbstractsInvertedIndex.txt.';
COMMENT ON COLUMN outs."PaperAbstractsInvertedIndex"."PaperId" IS 'FOREIGN KEY REFERENCES papers.papers_id';
COMMENT ON COLUMN outs."PaperAbstractsInvertedIndex"."IndexedAbstract" IS 'Inverted index, see https://en.wikipedia.org/wiki/Inverted_index';


COMMENT ON TABLE outs."PaperCitationContexts" IS 'FROZEN; citation contexts (nlp/PaperCitationContexts.txt)';
COMMENT ON COLUMN outs."PaperCitationContexts"."PaperId" IS 'FOREIGN KEY REFERENCES papers.papers_id';
COMMENT ON COLUMN outs."PaperCitationContexts"."PaperReferenceId" IS 'FOREIGN KEY REFERENCES papers.papers_id';


COMMENT ON TABLE outs."PaperResources" IS 'FROZEN; no longer updated. Data and code urls associated with papers (mag/PaperResources.txt)';
COMMENT ON COLUMN outs."PaperResources"."PaperId" IS 'FOREIGN KEY REFERENCES papers.papers_id';
COMMENT ON COLUMN outs."PaperResources"."ResourceType" IS 'Bit flags: 1=Project, 2=Data, 4=Code';
COMMENT ON COLUMN outs."PaperResources"."ResourceUrl" IS 'Url of resource';
COMMENT ON COLUMN outs."PaperResources"."SourceUrl" IS 'List of urls associated with the project, used to derive resource_url';
COMMENT ON COLUMN outs."PaperResources"."RelationshipType" IS 'Bit flags: 1=Own, 2=Cite';


COMMENT ON TABLE outs."Papers" IS 'Main data for papers (mag/Papers.txt)';
COMMENT ON COLUMN outs."Papers"."PaperId" IS 'PRIMARY KEY';
COMMENT ON COLUMN outs."Papers"."Rank" IS 'FROZEN; no new ranks are being added';
COMMENT ON COLUMN outs."Papers"."Doi" IS 'Doi values are upper-cased per DOI standard at https://www.doi.org/doi_handbook/2_Numbering.html#2.4';
COMMENT ON COLUMN outs."Papers"."DocType" IS 'Possible values: Book, BookChapter, Conference, Dataset, Journal, Patent, Repository, Thesis, NULL : unknown. Patent is FROZEN; no new Patents are being added.';
COMMENT ON COLUMN outs."Papers"."Genre" IS 'NEW; Crossref ontology for work type such as "journal-article, posted-content, dataset, or book-chapter';
COMMENT ON COLUMN outs."Papers"."IsParatext" IS 'NEW; indicates front-matter. See https://support.unpaywall.org/support/solutions/articles/44001894783';
COMMENT ON COLUMN outs."Papers"."PaperTitle" IS 'UPDATED; slightly different normalization algorithm';
COMMENT ON COLUMN outs."Papers"."JournalId" IS 'FOREIGN KEY references journals.journal_id';
COMMENT ON COLUMN outs."Papers"."ConferenceSeriesId" IS 'FROZEN; no longer updated, no new Conference Series are being added. FOREIGN KEY references conference_series.conference_series_id.';
COMMENT ON COLUMN outs."Papers"."ConferenceInstanceId" IS 'FROZEN; no longer updated, no new Conference Instances are being added. FOREIGN KEY references conference_instances.conference_instance_id.';
COMMENT ON COLUMN outs."Papers"."EstimatedCitation" IS 'UPDATED; new algorithm';
COMMENT ON COLUMN outs."Papers"."FamilyId" IS 'FROZEN; no longer updated.';
COMMENT ON COLUMN outs."Papers"."FamilyRank" IS 'FROZEN; no longer updated.';
COMMENT ON COLUMN outs."Papers"."DocSubTypes" IS 'Possible values: Retracted Publication, Retraction Notice';
COMMENT ON COLUMN outs."Papers"."OaStatus" IS 'NEW; Possible values: closed, green, gold, hybrid, bronze (see https://en.wikipedia.org/wiki/Open_access#Colour_naming_system)';
COMMENT ON COLUMN outs."Papers"."BestUrl" IS 'NEW; An url for the paper (see paper_urls table for more)';
COMMENT ON COLUMN outs."Papers"."BestFreeUrl" IS 'NEW; Url of best legal free-to-read copy when it exists (see https://support.unpaywall.org/support/solutions/articles/44001943223)';
COMMENT ON COLUMN outs."Papers"."BestFreeVersion" IS 'NEW; Possible values: submittedVersion, acceptedVersion, publishedVersion';
COMMENT ON COLUMN outs."Papers"."DoiLower" IS 'NEW; lowercase doi for convenience linking to Unpaywall';
COMMENT ON COLUMN outs."Papers"."UpdatedDate" IS 'NEW; set when changes are made going forward';


