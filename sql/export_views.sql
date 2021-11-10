------ mag_advanced_entity_related_entities

CREATE or replace view outs.entity_related_entities_view --- FROZEN; no longer updated. Relationship between papers.
--- DISTSTYLE key
--- distkey (entity_id)
--- sortkey (entity_id)
as (
    select
        entity_id,
        entity_type,            --- Possible values: af (Affiliation), j (Journal), c (Conference)
        related_entity_id,
        related_entity_type,    --- Possible values: af (Affiliation), j (Journal), c (Conference)
        related_type,           --- Possible values: 0 (same paper), 1 (common coauthors), 2 (co-cited), 3 (common field of study), 4 (same venue), 5 (A cites B), 6 (B cites A)
        score                   --- Confidence range between 0 and 1. Larger number representing higher confidence.
 from legacy.mag_advanced_entity_related_entities)
with no schema binding;


------ mag_advanced_field_of_study_children

CREATE or replace view outs.field_of_study_children_view --- Relationship between fields_of_study.
--- DISTSTYLE key
--- distkey (field_of_study_id)
--- sortkey (field_of_study_id)
as (
    select
        field_of_study_id,      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
        child_field_of_study_id --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
 from legacy.mag_advanced_field_of_study_children)
with no schema binding;


------ mag_advanced_field_of_study_extended_attributes

CREATE or replace view outs.field_of_study_extended_attributes_view --- Other identifiers for fields_of_study.
--- DISTSTYLE key
--- distkey (field_of_study_id)
--- sortkey (field_of_study_id)
as (
    select
        field_of_study_id,      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
        attribute_type,         --- Possible values: 1 (AUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsarchives04.html#2018AA), 2 (source url), 3 (CUI from UMLS https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html)
        attribute_value
 from legacy.mag_advanced_field_of_study_extended_attributes)
with no schema binding;

------ mag_advanced_fields_of_study

CREATE or replace view outs.fields_of_study_view --- Base table for Fields of Study
--- DISTSTYLE key
--- distkey (field_of_study_id)
--- sortkey (field_of_study_id)
as (
    with
        group_citations as (select field_of_study as field_of_study_id, count(*) as n from mid.citation cite join mid.work_concept work on work.paper_id = cite.paper_reference_id group by field_of_study),
        group_papers as (select field_of_study as field_of_study_id, count(distinct paper_id) as n from mid.work_concept group by field_of_study)
    select
        concept.field_of_study_id as field_of_study_id,      --- PRIMARY KEY
           rank,                           --- FROZEN; no new ranks are being added.
           normalized_name,                --- UPDATED; slightly different normalization algorithm
           display_name,
           main_type,
           level,                          --- Possible values: 0-5
           group_papers.n as paper_count,
           group_papers.n as paper_family_count, --- FROZEN; same value as paper_count.
           coalesce(group_citations.n, 0) as citation_count,
           create_date
    from mid.concept concept
 left outer join group_citations on group_citations.field_of_study_id = concept.field_of_study_id
 left outer join group_papers on group_papers.field_of_study_id = concept.field_of_study_id
   )
with no schema binding;


------ mag_advanced_paper_fields_of_study

CREATE or replace view outs.paper_fields_of_study_view --- Linking table from papers to fields, with score
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,                       --- FOREIGN KEY REFERENCES Papers.PaperId
           field_of_study as field_of_study_id,      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
           score,                          --- Confidence range between 0 and 1. Bigger number representing higher confidence.
           1 as algorithm_version          -- NEW; version of algorithm to assign fields. Possible values: 1=old MAG (no longer added), 2=OpenAlex
    from mid.work_concept
   )
with no schema binding;


------ mag_advanced_paper_mesh

CREATE or replace view outs.paper_mesh_view --- MeSH headings assigned to the paper by PubMed
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,          --- FOREIGN KEY REFERENCES Papers.PaperId
           descriptor_ui,     --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           descriptor_name,   --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           qualifier_ui,      --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           qualifier_name,    --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
           is_major_topc      --- see https://en.wikipedia.org/wiki/Medical_Subject_Headings
    from mid.mesh
   )
with no schema binding;


------ mag_advanced_paper_recommendations

CREATE or replace view outs.paper_recommendations_view --- Paper recommendations with score
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,                --- FOREIGN KEY REFERENCES Papers.PaperId
           recommended_paper_id,    --- FOREIGN KEY REFERENCES Papers.PaperId
           score                    --- Confidence range between 0 and 1. Bigger number representing higher confidence.
    from legacy.mag_advanced_paper_recommendations
   )
with no schema binding;


------ mag_advanced_related_field_of_study

CREATE or replace view outs.related_field_of_study_view --- Relationships between fields of study
--- DISTSTYLE key
--- distkey (field_of_study_id1)
--- sortkey (field_of_study_id1)
as (
    select
        field_of_study_id1,      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
           type1,                   --- Possible values: general, disease, disease_cause, medical_treatment, symptom
           field_of_study_id2,      --- FOREIGN KEY REFERENCES FieldsOfStudy.FieldOfStudyId
           type2,                   --- Possible values: general, disease, disease_cause, medical_treatment, symptom
           rank                     --- FROZEN; no new ranks are being added.
    from legacy.mag_advanced_related_field_of_study
   )
with no schema binding;


------ mag_main_affiliations

CREATE or replace view outs.affiliations_view --- Base table for affiliations (institutions)
--- DISTSTYLE key
--- distkey (affiliation_id)
--- sortkey (affiliation_id)
as (
        with
            group_citations as (select affiliation_id, count(*) as n from mid.citation cite join mid.affiliation affil on affil.paper_id = cite.paper_reference_id group by affiliation_id),
            group_papers as (select affiliation_id, count(distinct paper_id) as n from mid.affiliation group by affiliation_id)
    select
        affil.affiliation_id as affiliation_id,    --- PRIMARY KEY
       rank,                        --- FROZEN; no new ranks are being added.
       normalized_name,             --- UPDATED; slightly different normalization algorithm
       display_name,
       grid_id,                     --- FROZEN; ror_id is the new standard identifier for organizations
       ror_id,                      --- NEW; ROR for this organization, see https://ror.org, https://ror.org/:ror_id
       official_page,
       wiki_page,
       coalesce(group_papers.n, 0) as paper_count,
       coalesce(group_papers.n, 0) as paper_family_count,  --- FROZEN; same value as paper_count.
       coalesce(group_citations.n, 0) as citation_count,
       iso3166_code,              --- Two-letter country codes, see https://en.wikipedia.org/wiki/ISO_3166-2
       latitude,
       longitude,
       created_date,
       updated_date              --- NEW; set values updated from new ror data
    from mid.institution affil
        left outer join group_citations on group_citations.affiliation_id=affil.affiliation_id
        left outer join group_papers on group_papers.affiliation_id = affil.affiliation_id
   )
with no schema binding;

------ mag_main_author_extended_attributes

CREATE or replace view outs.author_extended_attributes_view --- Additional author name representations
--- DISTSTYLE key
--- distkey (author_id)
--- sortkey (author_id)
as (
    select
        author_id,             --- FOREIGN KEY REFERENCES Authors.AuthorId
           attribute_type,       --- Possible values: 1=Alternative name
           attribute_value
    from legacy.mag_main_author_extended_attributes
    )
with no schema binding;


------ mag_main_authors

CREATE or replace view outs.authors_view --- Base table for affiliations (institutions)
--- DISTSTYLE key
--- distkey (author_id)
--- sortkey (author_id)
as (
        with
            group_citations as (select author_id, count(*) as n from mid.citation cite join mid.affiliation affil on affil.paper_id = cite.paper_reference_id group by author_id),
            group_papers as (select author_id, count(distinct paper_id) as n from mid.affiliation group by author_id)
    select
        author.author_id as author_id,        --- PRIMARY KEY
       rank,                        --- FROZEN; no new ranks are being added
       normalized_name,           --- UPDATED; slightly different normalization algorithm
       display_name,
       author_orcid.orcid as orcid,                        --- NEW; ORCID identifier for this author
       last_known_affiliation_id,
       coalesce(group_papers.n, 0) as paper_count,
       coalesce(group_papers.n, 0) as paper_family_count,  --- FROZEN; same value as paper_count
       coalesce(group_citations.n, 0) as citation_count,
       created_date,
       updated_date              --- NEW; set when changes are made going forward
    from mid.author author
        left outer join group_citations on group_citations.author_id=author.author_id
        left outer join group_papers on group_papers.author_id = author.author_id
        left outer join mid.author_orcid on mid.author_orcid.author_id = author.author_id
   )
with no schema binding;


------ mag_main_conference_instances

CREATE or replace view outs.conference_instances_view --- Base table for Conference Instances
--- DISTSTYLE key
--- distkey (conference_instance_id)
--- sortkey (conference_instance_id)
as (
        with
            group_citations as (select conference_instance_id, count(*) as n from mid.work group by conference_instance_id),
            group_papers as (select conference_instance_id, count(*) as n from mid.work group by conference_instance_id)
    select
        inst.conference_instance_id as conference_instance_id, --- PRIMARY KEY
           normalized_name,                 --- UPDATED; slightly different normalization algorithm
           display_name,
           conference_series_id,            --- FOREIGN KEY REFERENCES ConferenceSeries.ConferenceSeriesId
           location,
           official_url,
           start_date,
           end_date,
           abstract_registration_date,
           submission_deadline_date,
           notification_due_date,
           final_version_due_date,
           coalesce(group_papers.n, 0) as paper_count,
           coalesce(group_papers.n, 0) as paper_family_count,  --- FROZEN; same value as paper_count
           coalesce(group_citations.n, 0) as citation_count,
           latitude,
           longitude,
           created_date
    from legacy.mag_main_conference_instances inst
        left outer join group_citations on group_citations.conference_instance_id=inst.conference_instance_id
        left outer join group_papers on group_papers.conference_instance_id = inst.conference_instance_id
   )
with no schema binding;


------ mag_main_conference_series

CREATE or replace view outs.conference_series_view --- Base table for Conference Series
--- DISTSTYLE key
--- distkey (conference_series_id)
--- sortkey (conference_series_id)
as (
        with
            group_citations as (select conference_series_id, count(*) as n from mid.work group by conference_series_id),
            group_papers as (select conference_series_id, count(*) as n from mid.work group by conference_series_id)
    select
        series.conference_series_id as conference_series_id,     --- PRIMARY KEY
           rank,                            --- FROZEN; no new ranks are being added
           normalized_name,                 --- UPDATED; slightly different normalization algorithm
           display_name,
           coalesce(group_papers.n, 0) as paper_count,
           coalesce(group_papers.n, 0) as paper_family_count,  --- FROZEN; same value as paper_count
           coalesce(group_citations.n, 0) as citation_count,
           created_date
    from legacy.mag_main_conference_series series
        left outer join group_citations on group_citations.conference_series_id=series.conference_series_id
        left outer join group_papers on group_papers.conference_series_id = series.conference_series_id
   )
with no schema binding;


------ mag_main_journals

CREATE or replace view outs.journals_view --- Base table for Journals
--- DISTSTYLE key
--- distkey (journal_id)
--- sortkey (journal_id)
as (
        with
            group_citations as (select journal_id, count(*) as n from mid.citation cite join mid.work work on work.paper_id = cite.paper_reference_id group by journal_id),
            group_papers as (select journal_id, count(distinct paper_id) as n from mid.work group by journal_id)
    select
        mid.journal.journal_id as journal_id,     --- PRIMARY KEY
       rank,                    --- FROZEN; no new ranks are being added
       normalized_name,         --- UPDATED; slightly different normalization algorithm
       display_name,
       issn,                    --- the ISSN-L for the journal (see https://en.wikipedia.org/wiki/International_Standard_Serial_Number#Linking_ISSN)
       issns,                   --- NEW; JSON list of all ISSNs for this journal (example: \'["1469-5073","0016-6723"]\' )
       is_oa,                   --- NEW; TRUE when the journal is 100% OA
       is_in_doaj,              --- NEW; TRUE when the journal is in DOAJ (see https://doaj.org/)
       publisher,
       webpage,
       coalesce(group_papers.n, 0) as paper_count,
       coalesce(group_papers.n, 0) as paper_family_count, --- FROZEN; same value as paper_count
       coalesce(group_citations.n, 0) as citation_count,
       created_date,
       updated_date              --- NEW; set when changes are made going forward
    from mid.journal
        left outer join group_citations on group_citations.journal_id=mid.journal.journal_id
        left outer join group_papers on group_papers.journal_id = mid.journal.journal_id
   )
with no schema binding;


------ mag_main_paper_author_affiliations

CREATE or replace view outs.paper_author_affiliations_view --- Links between papers, authors, and institutions. NOTE: It is possible to have multiple rows with same (PaperId, AuthorId, AffiliationId) when an author is associated with multiple affiliations.
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,                --- FOREIGN KEY REFERENCES Papers.PaperId
           author_id,               --- FOREIGN KEY REFERENCES Authors.AuthorId
           affiliation_id,          --- FOREIGN KEY REFERENCES Affiliations.AffiliationId
           author_sequence_number,  --- 1-based author sequence number. 1: the 1st author listed on paper, 2: the 2nd author listed on paper, etc.
           original_author,
           original_affiliation
    from mid.affiliation
   )
with no schema binding;

------ mag_main_paper_extended_attributes

CREATE or replace view outs.paper_extended_attributes_view --- Extra paper identifiers
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,             --- FOREIGN KEY REFERENCES Papers.PaperId
           attribute_type,       --- Possible values: 1=PatentId, 2=PubMedId, 3=PmcId, 4=Alternative Title
           attribute_value
    from mid.work_extra_ids
   )
with no schema binding;



------ mag_main_paper_references_id

CREATE or replace view outs.paper_references_view --- Paper references (and also, in reverse, citations)
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,                --- FOREIGN KEY REFERENCES Papers.PaperId
           paper_reference_id       --- FOREIGN KEY REFERENCES Papers.PaperId
    from mid.citation
   )
with no schema binding;

------ mag_main_paper_urls

CREATE or replace view outs.paper_urls_view --- MeSH headings assigned to the paper by PubMed
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,          --- FOREIGN KEY REFERENCES Papers.PaperId
           source_type,       --- Possible values: 1=Html, 2=Text, 3=Pdf, 4=Doc, 5=Ppt, 6=Xls, 8=Rtf, 12=Xml, 13=Rss, 20=Swf, 27=Ics, 31=Pub, 33=Ods, 34=Odp, 35=Odt, 36=Zip, 40=Mp3, 0/999/NULL=unknown
           source_url,
           language_code,
            url_for_landing_page,   --- NEW; URL for the landing page, when article is free to read
            url_for_pdf,            --- NEW; URL for the PDF, when article is free to read
            host_type,              --- NEW; host type of the free-to-read URL, Possible values: publisher, repository
            version,                --- NEW; version of the free-to-read URL Possible values: submittedVersion, acceptedVersion, publishedVersion
            license,                --- NEW; license of the free-to-read URL (example: cc0, cc-by, publisher-specific)
            repository_institution, --- NEW; name of repository host of URL
            pmh_id as oai_pmh_id    --- NEW; OAH-PMH id of the repository record
    from mid.location
   )
with no schema binding;


------ mag_nlp_paper_abstracts_inverted

CREATE or replace view outs.paper_abstracts_inverted_view --- Inverted abstracts
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        work_id as paper_id,       --- FOREIGN KEY REFERENCES papers.papers_id
        abstract as indexed_abstract    --- Inverted index, see https://en.wikipedia.org/wiki/Inverted_index
 from mid.abstract)
with no schema binding;


------ mag_nlp_paper_citation_contexts

CREATE or replace view outs.paper_citation_contexts_view --- FROZEN; citation contexts
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,               --- FOREIGN KEY REFERENCES papers.papers_id
        paper_reference_id      --- FOREIGN KEY REFERENCES papers.papers_id
        citation_context        ---
 from mid.citation_contexts)
with no schema binding;


------ mag_main_paper_resources


CREATE or replace view outs.paper_resources_view --- FROZEN; no longer updated. Data and code urls associated with papers
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    select
        paper_id,       --- FOREIGN KEY REFERENCES papers.papers_id
        resource_type,  --- Bit flags: 1=Project, 2=Data, 4=Code
        resource_url,   --- Url of resource
        source_url,     --- List of urls associated with the project, used to derive resource_url
        relationship_type --- Bit flags: 1=Own, 2=Cite
 from legacy.mag_main_paper_resources)
with no schema binding;


------ mag_main_papers

CREATE or replace view outs.papers_view --- Main data for papers
--- DISTSTYLE key
--- distkey (paper_id)
--- sortkey (paper_id)
as (
    with
        reference_count as (select paper_id as citing_paper_id, count(*) as n from mid.citation group by paper_id),
        citation_count as (select paper_reference_id as cited_paper_id, count(*) as n from mid.citation group by paper_reference_id)
    select
        paper_id,       -- PRIMARY KEY
        rank,           --- FROZEN; no new ranks are being added
        doi,            --- Doi values are upper-cased per DOI standard at https://www.doi.org/doi_handbook/2_Numbering.html#2.4
        doc_type,       --- Possible values: Book, BookChapter, Conference, Dataset, Journal, Patent, Repository, Thesis, NULL : unknown. Patent is FROZEN; no new Patents are being added.
        genre,          --- NEW
        is_paratext,    --- NEW
        paper_title,    --- UPDATED; slightly different normalization algorithm
        original_title,
        book_title,
        year,
        publication_date,
        online_date,
        publisher,
        journal_id,     --- FOREIGN KEY references journals.journal_id
        conference_series_id, --- FROZEN; no longer updated, no new Conference Series are being added. FOREIGN KEY references conference_series.conference_series_id.
        conference_instance_id, --- FROZEN; no longer updated, no new Conference Instances are being added. FOREIGN KEY references conference_instances.conference_instance_id.
        volume,
        issue,
        first_page,
        last_page,
        coalesce(reference_count.n, 0) as reference_count,
        coalesce(citation_count.n, 0) as citation_count,
        coalesce(citation_count.n, 0) as estimated_citation, --- UPDATED; new algorithm
        original_venue,
        family_id,          --- FROZEN; no longer updated.
        family_rank,        --- FROZEN; no longer updated.
        doc_sub_types,      --- Possible values: "Retracted Publication", "Retraction Notice".
        oa_status,          --- NEW; Possible values: closed, green, gold, hybrid, bronze (see https://en.wikipedia.org/wiki/Open_access#Colour_naming_system)
        best_url,           --- NEW; An url for the paper (see paper_urls table for more)
        best_free_url,      --- NEW; Url of best legal free-to-read copy when it exists (see https://support.unpaywall.org/support/solutions/articles/44001943223)
        best_free_version,  --- NEW; Possible values: submittedVersion, acceptedVersion, publishedVersion
        doi_lower,          --- NEW; lowercase doi for convenience linking to Unpaywall
        created_date,
        updated_date,       --- NEW; set when changes are made going forward
    from mid.work work
    left outer join reference_count on work.paper_id=reference_count.citing_paper_id
    left outer join citation_count on work.paper_id=citation_count.cited_paper_id
)
with no schema binding;
