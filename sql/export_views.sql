-- making the views is very fast
-- then run
-- python sql_generate_export_tables.py  -i export_views.sql -o export_tables_generated.sql
-- making the tables and comments currently takes about 20 minutes (1400 seconds)
-- doing the unloads takes about 25 minutes (4000 seconds)

set enable_case_sensitive_identifier=true;

------ mag_advanced_entity_related_entities

create or replace view outs."EntityRelatedEntities_view" --- Relationship between papers, authors, fields of study. (advanced/EntityRelatedEntities.txt)
--- DISTSTYLE key
--- DISTKEY ("EntityId")
--- SORTKEY ("EntityId")
as (
    select
        entity_id as "EntityId",
        entity_type as "EntityType",            --- Possible values: af (Affiliation), j (Journal), c (Conference)
        related_entity_id as "RelatedEntityId",
        related_entity_type as "RelatedEntityType",  --- Possible values: af (Affiliation), j (Journal), c (Conference)
        related_type as "RelatedType",     --- Possible values: 0 (same paper), 1 (common coauthors), 2 (co-cited), 3 (common field of study), 4 (same venue), 5 (A cites B), 6 (B cites A)
        score as "Score"                   --- Confidence range between 0 and 1. Larger number representing higher confidence.
 from legacy.mag_advanced_entity_related_entities)
with no schema binding;


------ mag_advanced_field_of_study_children

create or replace view outs."FieldOfStudyChildren_view" --- Relationship between Fields of Study (advanced/FieldOfStudyChildren.txt)
--- DISTSTYLE key
--- DISTKEY ("FieldOfStudyId")
--- SORTKEY ("FieldOfStudyId")
as (
    select
        field_of_study_id as "FieldOfStudyId",      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
        child_field_of_study_id as "ChildFieldOfStudyId" --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
 from legacy.mag_advanced_field_of_study_children)
with no schema binding;


------ mag_advanced_field_of_study_extended_attributes

create or replace view outs."FieldOfStudyExtendedAttributes_view" --- Other identifiers for Fields of Study (advanced/FieldOfStudyExtendedAttributes.txt)
--- DISTSTYLE key
--- DISTKEY ("FieldOfStudyId")
--- SORTKEY ("FieldOfStudyId")
as (
    select
        field_of_study_id as "FieldOfStudyId",      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
        attribute_type as "AttributeType",         --- Possible values: 1 (AUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsarchives04.html#2018AA), 2 (source url), 3 (CUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html)
        attribute_value as "AttributeValue"
 from legacy.mag_advanced_field_of_study_extended_attributes)
with no schema binding;

------ mag_advanced_fields_of_study

create or replace view outs."FieldsOfStudy_view" --- Base table for Fields of Study (advanced/FieldsOfStudy.txt)
--- DISTSTYLE key
--- DISTKEY ("FieldOfStudyId")
--- SORTKEY ("FieldOfStudyId")
as (
    select
        concept.field_of_study_id as "FieldOfStudyId",      --- PRIMARY KEY
           rank as "Rank",                           --- FROZEN; no new ranks are being added.
           normalized_name as "NormalizedName",      --- UPDATED; slightly different normalization algorithm
           display_name as "DisplayName",
           main_type as "MainType",
           level as "Level",                          --- Possible values: 0-5
           paper_count as "PaperCount",
           paper_count as "PaperFamilyCount",      --- FROZEN; same value as PaperCount.
           coalesce(group_citations.n, 0) as "CitationCount",
           created_date as "CreatedDate"
    from mid.concept concept
   )
with no schema binding;


------ mag_advanced_paper_fields_of_study

create or replace view outs."PaperFieldsOfStudy_view" --- Linking table from papers to fields, with score (advanced/PaperFieldsOfStudy.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",                    --- FOREIGN KEY REFERENCES Papers.PaperId
           field_of_study as "FieldOfStudyId",    --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
           score as "Score",                      --- Confidence range between 0 and 1. Bigger number representing higher confidence.
           1 as "AlgorithmVersion"                -- NEW; version of algorithm to assign fields. Possible values: 1=old MAG (FROZEN), 2=OpenAlex
    from mid.work_concept t1
   )
with no schema binding;


------ mag_advanced_paper_mesh

create or replace view outs."PaperMeSH_view" --- MeSH headings assigned to the paper by PubMed (advanced/PaperMeSH.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",          --- FOREIGN KEY REFERENCES Papers.PaperId
           descriptor_ui as "DescriptorUI",     --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           descriptor_name as "DescriptorName",   --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           qualifier_ui as "QualifierUI",      --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           qualifier_name as "QualifierName",    --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           is_major_topic as "IsMajorTopic"     --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
    from mid.mesh t1
   )
with no schema binding;


------ mag_advanced_paper_recommendations

create or replace view outs."PaperRecommendations_view" --- Paper recommendations with score (advanced/PaperRecommendations.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",                --- FOREIGN KEY REFERENCES Papers.PaperId
           recommended_paper_id as "RecommendedPaperId",    --- FOREIGN KEY REFERENCES Papers.PaperId
           score as "Score"                    --- Confidence range between 0 and 1. Bigger number representing higher confidence.
    from legacy.mag_advanced_paper_recommendations t1
   )
with no schema binding;


------ mag_advanced_related_field_of_study

create or replace view outs."RelatedFieldOfStudy_view" --- Relationships between fields of study (advanced/RelatedFieldOfStudy.txt)
--- DISTSTYLE key
--- DISTKEY ("FieldOfStudyId1")
--- SORTKEY ("FieldOfStudyId1")
as (
    select
        field_of_study_id1 as "FieldOfStudyId1",      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
           type1 as "Type1",                   --- Possible values: general, disease, disease_cause, medical_treatment, symptom
           field_of_study_id2 as "FieldOfStudyId2",      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
           type2 as "Type2",                   --- Possible values: general, disease, disease_cause, medical_treatment, symptom
           rank as "Rank"                      --- FROZEN; no new ranks are being added.
    from legacy.mag_advanced_related_field_of_study
   )
with no schema binding;


------ mag_main_affiliations

create or replace view outs."Affiliations_view" --- Base table for affiliations/institutions (mag/Affiliations.txt)
--- DISTSTYLE key
--- DISTKEY ("AffiliationId")
--- SORTKEY ("AffiliationId")
as (
    select
       affil.affiliation_id as "AffiliationId",    --- PRIMARY KEY
       rank as "Rank",                        --- FROZEN; no new ranks are being added.
       normalized_name as "NormalizedName",             --- UPDATED; slightly different normalization algorithm
       display_name as "DisplayName",
       grid_id as "GridId",                     --- FROZEN; RorId is the new standard identifier for organizations
       ror_id as "RorId",                      --- NEW; ROR for this organization, see https://ror.org, https://ror.org/:RorId
       official_page as "OfficialPage",
       wiki_page as "WikiPage",
       paper_count as "PaperCount",
       paper_count as "PaperFamilyCount",  --- FROZEN; same value as PaperCount.
       citation_count as "CitationCount",
       iso3166_code as "Iso3166Code",              --- Two-letter country codes, see https://en.wikipedia.org/wiki/ISO_3166-2
       latitude as "Latitude",
       longitude as "Longitude",
       created_date as "CreatedDate",
       updated_date as "UpdatedDate"              --- NEW; set values updated from new ror data
    from mid.institution affil
   )
with no schema binding;

------ mag_main_author_extended_attributes

create or replace view outs."AuthorExtendedAttributes_view" --- Additional author name representations (mag/AuthorExtendedAttributes.txt)
--- DISTSTYLE key
--- DISTKEY ("AuthorId")
--- SORTKEY ("AuthorId")
as (
    select
        t1.author_id as "AuthorId",             --- FOREIGN KEY REFERENCES Authors.AuthorId
           attribute_type as "AttributeType",       --- Possible values: 1=Alternative name
           attribute_value as "AttributeValue"
    from legacy.mag_main_author_extended_attributes t1
    )
with no schema binding;


------ mag_main_authors

create or replace view outs."Authors_view" --- Base table for authors (mag/Authors.txt)
--- DISTSTYLE key
--- DISTKEY ("AuthorId")
--- SORTKEY ("AuthorId")
as (
        with
            group_orcids as (select author_id, max(orcid) as orcid from mid.author_orcid group by author_id)
    select
        author.author_id as "AuthorId",        --- PRIMARY KEY
       rank as "Rank",                        --- FROZEN; no new ranks are being added
       normalized_name as "NormalizedName",           --- UPDATED; slightly different normalization algorithm
       display_name as "DisplayName",
       group_orcids.orcid as "Orcid",                        --- NEW; ORCID identifier for this author (see https://orcid.org)
       last_known_affiliation_id as "LastKnownAffiliationId",
       paper_count as "PaperCount",
       paper_count as "PaperFamilyCount",  --- FROZEN; same value as PaperCount
       citationcount as "CitationCount",
       created_date as "CreatedDate",
       updated_date as "UpdatedDate"              --- NEW; set when changes are made going forward
    from mid.author author
        left outer join group_orcids on group_orcids.author_id = author.author_id
   )
with no schema binding;


------ mag_main_conference_instances

create or replace view outs."ConferenceInstances_view" --- FROZEN; Base table for Conference Instances (mag/ConferenceInstances.txt)
--- DISTSTYLE key
--- DISTKEY ("ConferenceInstanceId")
--- SORTKEY ("ConferenceInstanceId")
as (
    select
        inst.conference_instance_id as "ConferenceInstanceId", --- PRIMARY KEY
           normalized_name as "NormalizedName",                 --- UPDATED; slightly different normalization algorithm
           display_name as "DisplayName",
           conference_series_id as "ConferenceSeriesId",            --- FOREIGN KEY REFERENCES ConferenceSeries.ConferenceSeriesId
           location as "Location",
           official_url as "OfficialUrl",
           start_date as "StartDate",
           end_date as "EndDate",
           abstract_registration_date as "AbstractRegistrationDate",
           submission_deadline_date as "SubmissionDeadlineDate",
           notification_due_date as "NotificationDueDate",
           final_version_due_date as "FinalVersionDueDate",
           paper_count as "PaperCount",
           paper_count as "PaperFamilyCount",  --- FROZEN; same value as PaperCount
           citation_count as "CitationCount",
           latitude as "Latitude",
           longitude as "Longitude",
           created_date as "CreatedDate"
    from legacy.mag_main_conference_instances inst
   )
with no schema binding;


------ mag_main_conference_series

create or replace view outs."ConferenceSeries_view" --- FROZEN; Base table for Conference Series (mag/ConferenceSeries.txt)
--- DISTSTYLE key
--- DISTKEY ("ConferenceSeriesId")
--- SORTKEY ("ConferenceSeriesId")
as (
    select
        series.conference_series_id as "ConferenceSeriesId",     --- PRIMARY KEY
           rank as "Rank",                            --- FROZEN; no new ranks are being added
           normalized_name as "NormalizedName",                 --- UPDATED; slightly different normalization algorithm
           display_name as "DisplayName",
           paper_count as "PaperCount",
           paper_count as "PaperFamilyCount",  --- FROZEN; same value as PaperCount
           citation_count as "CitationCount",
           created_date as "CreatedDate"
    from legacy.mag_main_conference_series series
   )
with no schema binding;


------ mag_main_journals

create or replace view outs."Journals_view" --- Base table for Journals (mag/Journals.txt)
--- DISTSTYLE key
--- DISTKEY ("JournalId")
--- SORTKEY ("JournalId")
as (
    select
        mid.journal.journal_id as "JournalId",     --- PRIMARY KEY
       rank as "Rank",                    --- FROZEN; no new ranks are being added
       normalized_name as "NormalizedName",         --- UPDATED; slightly different normalization algorithm
       display_name as "DisplayName",
       issn as "Issn",                    --- UPDATED; the ISSN-L for the journal (see https://en.wikipedia.org/wiki/International_Standard_Serial_Number#Linking_ISSN)
       issns as "Issns",                   --- NEW; JSON list of all ISSNs for this journal (example: \'["1469-5073","0016-6723"]\' )
       is_oa as "IsOa",                   --- NEW; TRUE when the journal is 100% OA
       is_in_doaj as "IsInDoaj",              --- NEW; TRUE when the journal is in DOAJ (see https://doaj.org/)
       publisher as "Publisher",
       webpage as "Webpage",
       paper_count as "PaperCount",
       paper_count as "PaperFamilyCount", --- FROZEN; same value as PaperCount
       citation_count as "CitationCount",
       created_date as "CreatedDate",
       updated_date as "UpdatedDate"              --- NEW; set when changes are made going forward
    from mid.journal
   )
with no schema binding;


------ mag_main_paper_author_affiliations

create or replace view outs."PaperAuthorAffiliations_view" --- Links between papers, authors, and affiliations/institutions. NOTE: It is possible to have multiple rows with same (PaperId, AuthorId, AffiliationId) when an author is associated with multiple affiliations. (mag/PaperAuthorAffiliations.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",                --- FOREIGN KEY REFERENCES Papers.PaperId
           t1.author_id as "AuthorId",               --- FOREIGN KEY REFERENCES Authors.AuthorId
           t1.affiliation_id as "AffiliationId",          --- FOREIGN KEY REFERENCES Affiliations.AffiliationId
           author_sequence_number as "AuthorSequenceNumber",  --- 1-based author sequence number. 1: the 1st author listed on paper, 2: the 2nd author listed on paper, etc.
           original_author as "OriginalAuthor",
           original_affiliation as "OriginalAffiliation"
    from mid.affiliation t1
   )
with no schema binding;

------ mag_main_paper_extended_attributes

create or replace view outs."PaperExtendedAttributes_view" --- Extra paper identifiers (mag/PaperExtendedAttributes.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",             --- FOREIGN KEY REFERENCES Papers.PaperId
           attribute_type as "AttributeType",       --- Possible values: 1=PatentId, 2=PubMedId, 3=PmcId, 4=Alternative Title
           attribute_value as "AttributeValue"
    from mid.work_extra_ids t1
   )
with no schema binding;



------ mag_main_paper_references_id

create or replace view outs."PaperReferences_view" --- Paper references and, in reverse, citations (mag/PaperReferences.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",                --- FOREIGN KEY REFERENCES Papers.PaperId
        paper_reference_id as "PaperReferenceId"      --- FOREIGN KEY REFERENCES Papers.PaperId
    from mid.citation t1
   )
with no schema binding;

------ mag_main_paper_urls

create or replace view outs."PaperUrls_view" --- Urls for the paper (mag/PaperUrls.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",          --- FOREIGN KEY REFERENCES Papers.PaperId
           source_type as "SourceType",       --- Possible values: 1=Html, 2=Text, 3=Pdf, 4=Doc, 5=Ppt, 6=Xls, 8=Rtf, 12=Xml, 13=Rss, 20=Swf, 27=Ics, 31=Pub, 33=Ods, 34=Odp, 35=Odt, 36=Zip, 40=Mp3, 0/999/NULL=unknown
           source_url as "SourceUrl",
           language_code as "LanguageCode",
            url_for_landing_page as "UrlForLandingPage",   --- NEW; URL for the landing page, when article is free to read
            url_for_pdf as "UrlForPdf",            --- NEW; URL for the PDF, when article is free to read
            host_type as "HostType",              --- NEW; host type of the free-to-read URL, Possible values: publisher, repository
            version as "Version",                --- NEW; version of the free-to-read URL, Possible values: submittedVersion, acceptedVersion, publishedVersion (see https://support.unpaywall.org/support/solutions/articles/44000708792)
            license as "License",                --- NEW; license of the free-to-read URL (example: cc0, cc-by, publisher-specific)
            repository_institution as "RepositoryInstitution", --- NEW; name of repository host of URL
            pmh_id as "OaiPmhId"    --- NEW; OAH-PMH id of the repository record
    from mid.location t1
   )
with no schema binding;


------ mag_nlp_paper_abstracts_inverted

create or replace view outs."PaperAbstractsInvertedIndex_view" --- Inverted abstracts (nlp/PaperAbstractsInvertedIndex.txt.{*})
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",       --- FOREIGN KEY REFERENCES Papers.PapersId
        indexed_abstract as "IndexedAbstract"    --- Inverted index, see https://en.wikipedia.org/wiki/Inverted_index
    from mid.abstract t1
 )
with no schema binding;


------ mag_nlp_paper_citation_contexts

create or replace view outs."PaperCitationContexts_view" --- FROZEN; citation contexts (nlp/PaperCitationContexts.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",     --- FOREIGN KEY REFERENCES Papers.PapersId
        paper_reference_id as "PaperReferenceId",     --- FOREIGN KEY REFERENCES Papers.PapersId
        citation_context as "CitationContext"
    from legacy.mag_nlp_paper_citation_contexts t1
 )
with no schema binding;


------ mag_main_paper_resources


create or replace view outs."PaperResources_view" --- FROZEN; no longer updated. Data and code urls associated with papers (mag/PaperResources.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        t1.paper_id as "PaperId",       --- FOREIGN KEY REFERENCES Papers.PapersId
        resource_type as "ResourceType",  --- Bit flags: 1=Project, 2=Data, 4=Code
        resource_url as "ResourceUrl",   --- Url of resource
        source_url as "SourceUrl",     --- List of urls associated with the project, used to derive resource_url
        relationship_type as "RelationshipType"--- Bit flags: 1=Own, 2=Cite
 from legacy.mag_main_paper_resources t1
 )
with no schema binding;


------ mag_main_papers

create or replace view outs."Papers_view" --- Main data for papers (mag/Papers.txt)
--- DISTSTYLE key
--- DISTKEY ("PaperId")
--- SORTKEY ("PaperId")
as (
    select
        work.paper_id as "PaperId",       -- PRIMARY KEY
        rank as "Rank",           --- FROZEN; no new ranks are being added
        doi as "Doi",            --- Doi values are upper-cased per DOI standard at https://www.doi.org/doi_handbook/2_Numbering.html#2.4
        doc_type as "DocType",       --- Possible values: Book, BookChapter, Conference, Dataset, Journal, Repository, Thesis, NULL : unknown. Patent is FROZEN; no new Patents are being added.
        genre as "Genre",          --- NEW; Crossref ontology for work type such as journal-article, posted-content, dataset, or book-chapter
        is_paratext as "IsParatext",    --- NEW; indicates front-matter. See https://support.unpaywall.org/support/solutions/articles/44001894783
        paper_title as "PaperTitle",    --- UPDATED; slightly different normalization algorithm
        original_title as "OriginalTitle",
        book_title as "BookTitle",
        year as "Year",
        publication_date as "Date",
        online_date as "OnlineDate",
        publisher as "Publisher",
        journal_id as "JournalId",     --- FOREIGN KEY references Journals.JournalId
        conference_series_id as "ConferenceSeriesId", --- FROZEN; no longer updated, no new Conference Series are being added. FOREIGN KEY references ConferenceSeries.ConferenceSeriesId.
        conference_instance_id as "ConferenceInstanceId", --- FROZEN; no longer updated, no new Conference Instances are being added. FOREIGN KEY references ConferenceInstances.ConferenceInstanceId
        volume as "Volume",
        issue as "Issue",
        first_page as "FirstPage",
        last_page as "LastPage",
        reference_count as "ReferenceCount",
        citation_count as "CitationCount",
        estimated_citation as "EstimatedCitation", --- UPDATED; new algorithm
        original_venue as "OriginalVenue",
        family_id as "FamilyId",          --- FROZEN; no longer updated.
        family_rank as "FamilyRank",        --- FROZEN; no longer updated.
        doc_sub_types as "DocSubTypes",      --- Possible values: Retracted Publication, Retraction Notice
        oa_status as "OaStatus",          --- NEW; Possible values: closed, green, gold, hybrid, bronze (see https://en.wikipedia.org/wiki/Open_access#Colour_naming_system)
        best_url as "BestUrl",           --- NEW; An url for the paper (see PaperUrls table for more)
        best_free_url as "BestFreeUrl",      --- NEW; Url of best legal free-to-read copy when it exists (see https://support.unpaywall.org/support/solutions/articles/44001943223)
        best_free_version as "BestFreeVersion",  --- NEW; Possible values: submittedVersion, acceptedVersion, publishedVersion (see https://support.unpaywall.org/support/solutions/articles/44000708792)
        doi_lower as "DoiLower",          --- NEW; lowercase doi for convenience linking to Unpaywall
        created_date as "CreatedDate",
        updated_date as "UpdatedDate"       --- NEW; set when changes are made going forward
    from mid.work work
)
with no schema binding;
