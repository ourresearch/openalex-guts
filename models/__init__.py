from sqlalchemy import and_, or_, orm, func
from sqlalchemy.orm import foreign, remote, selectinload

from app import db
from models.abstract import Abstract
from models.affiliation import Affiliation
from models.author import Author
from models.author_alternative_name import AuthorAlternativeName
from models.author_concept import AuthorConcept
from models.author_topic import AuthorTopic
from models.author_orcid import AuthorOrcid
from models.citation import Citation, CitationUnmatched
from models.concept import Concept
from models.concept_ancestor import ConceptAncestor
from models.counts import AuthorCountsByYearPapers, AuthorCountsByYearCitations
from models.counts import CitationPercentilesByYear
from models.counts import ConceptCountsByYear
from models.counts import InstitutionCountsByYearPapers, InstitutionCountsByYearCitations
from models.counts import SourceCountsByYearPapers, SourceCountsByYearCitations
from models.counts import WorkCountsByYear
from models.country import Country
from models.continent import Continent
from models.doi_ra import DOIRegistrationAgency
from models.funder import Funder, WorkFunder
from models.institution import Institution, InstitutionAssertions, AffiliationStringCuration
from models.institution import InstitutionAncestors
from models.institution_topic import InstitutionTopic
from models.institution_type import InstitutionType
from models.issn_to_issnl import ISSNtoISSNL
from models.keyword import Keyword
from models.language import Language
from models.license import License
from models.location import Location
from models.mesh import Mesh
from models.work_openapc import WorkOpenAPC
from models.work_embedding import WorkEmbedding
from models.work_sdg import WorkSDG
from models.orcid import Orcid
from models.publisher import Publisher
from models.record import Record
from models.retraction_watch import RetractionWatch
from models.ror import Ror
from models.ror_matching import RORGapInstitution
from models.source import Source
from models.source_language_override import SourceLanguageOverride
from models.summary_stats import AuthorImpactFactor, AuthorHIndex, AuthorI10Index, AuthorI10Index2Year, AuthorHIndex2Year
from models.summary_stats import ConceptImpactFactor, ConceptHIndex, ConceptI10Index, ConceptI10Index2Year, ConceptHIndex2Year
from models.summary_stats import FunderImpactFactor, FunderHIndex, FunderI10Index, FunderI10Index2Year, FunderHIndex2Year
from models.summary_stats import InstitutionImpactFactor, InstitutionHIndex, InstitutionI10Index, InstitutionI10Index2Year, InstitutionHIndex2Year
from models.summary_stats import PublisherImpactFactor, PublisherHIndex, PublisherI10Index, PublisherI10Index2Year
from models.summary_stats import SourceImpactFactor, SourceHIndex, SourceI10Index, SourceI10Index2Year, SourceHIndex2Year
from models.topic import Topic
from models.subfield import Subfield
from models.field import Field
from models.domain import Domain
from models.sdg import SDG
from models.source_type import SourceType
from models.source_topic import SourceTopic
from models.work_type import WorkType
from models.work_related_version import WorkRelatedVersion
from models.unpaywall import Unpaywall
from models.work import Work
from models.work_keyword import WorkKeyword
from models.work_concept import WorkConcept
from models.work_topic import WorkTopic
from models.work_extra_id import WorkExtraIds
from models.work_related_work import WorkRelatedWork
from models.work_fwci import WorkFWCI
from models.work_citations_normalized_percentile import WorkCitationNormPer
from util import normalize

REDIS_WORK_QUEUE = 'queue:work_store'
REDIS_ADD_THINGS_QUEUE = 'queue:add_things'

# relationships without association tables
Work.mesh = db.relationship("Mesh", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.references = db.relationship("Citation", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.references_unmatched = db.relationship("CitationUnmatched", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.locations = db.relationship("Location", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.abstract = db.relationship("Abstract", lazy='selectin', backref="work", uselist=False, cascade="all, delete-orphan")
Work.journal = db.relationship("Source", lazy='selectin', backref="work", uselist=False) #don't delete orphan
Work.extra_ids = db.relationship("WorkExtraIds", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.related_works = db.relationship("WorkRelatedWork", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.records = db.relationship("Record", lazy='selectin', backref="work")  # normally don't get, just for add_everything
WorkFunder.funder = db.relationship("Funder", lazy='selectin', uselist=False)
Work.openapc = db.relationship("WorkOpenAPC", uselist=False)
Work.embeddings = db.relationship("WorkEmbedding", uselist=False)
Work.sdg = db.relationship("WorkSDG", uselist=False)
Work.doi_ra = db.relationship("DOIRegistrationAgency", lazy='selectin', uselist=False)
Work.retraction_watch = db.relationship("RetractionWatch", lazy='selectin', uselist=False)
Work.work_fwci = db.relationship("WorkFWCI", lazy='selectin', uselist=False)
Work.work_citations_norm_percentile = db.relationship("WorkCitationNormPer", lazy='selectin', uselist=False)
Work.institution_assertions = db.relationship("InstitutionAssertions", lazy='selectin', cascade="all, delete-orphan")
InstitutionAssertions.institution = db.relationship("Institution", lazy='selectin', uselist=False)
Work.institution_curation_requests = db.relationship("AffiliationStringCuration", lazy='selectin', cascade="all, delete-orphan")

Work.related_versions = db.relationship(
    "WorkRelatedVersion",
    lazy="selectin",
    primaryjoin="Work.paper_id==WorkRelatedVersion.work_id",
    uselist=True,
)
WorkRelatedVersion.related_work = db.relationship("Work", foreign_keys=[WorkRelatedVersion.version_work_id], lazy='selectin', uselist=False)

Work.datasets = db.relationship(
    "WorkRelatedVersion",
    lazy="selectin",
    backref="work",
    primaryjoin="Work.paper_id==WorkRelatedVersion.version_work_id",
    uselist=True,
    viewonly=True
)
WorkRelatedVersion.related_dataset = db.relationship("Work", foreign_keys=[WorkRelatedVersion.work_id], lazy='selectin', uselist=False, viewonly=True)

# relationships with association tables
Work.affiliations = db.relationship("Affiliation", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.concepts = db.relationship("WorkConcept", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.topics = db.relationship("WorkTopic", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.keywords = db.relationship("WorkKeyword", lazy='selectin', backref="work", cascade="all, delete-orphan")
Work.funders = db.relationship("WorkFunder", lazy='selectin', cascade="all, delete-orphan")

Affiliation.author = db.relationship("Author", lazy='selectin', backref='affiliations') # don't delete orphan
Affiliation.institution = db.relationship("Institution", lazy='selectin') #don't delete orphan

Topic.subfield = db.relationship("Subfield", lazy='selectin', backref="topics", uselist=False)
Topic.field = db.relationship("Field", lazy='selectin', backref="topics", uselist=False)
Topic.domain = db.relationship("Domain", lazy='selectin', backref="topics", uselist=False)
Institution.ror = db.relationship("Ror", uselist=False)
Author.orcids = db.relationship("AuthorOrcid", backref="author", cascade="all, delete-orphan")
AuthorOrcid.orcid_data = db.relationship("Orcid", uselist=False)
Author.alternative_names = db.relationship("AuthorAlternativeName", cascade="all, delete-orphan")
Author.author_concepts = db.relationship("AuthorConcept", cascade="all, delete-orphan")


Author.author_topics = db.relationship("AuthorTopic", cascade="all, delete-orphan")
Institution.institution_topics = db.relationship("InstitutionTopic", lazy='selectin', cascade="all, delete-orphan")
Source.source_topics = db.relationship("SourceTopic", cascade="all, delete-orphan")

# Concept.works = db.relationship("WorkConcept", lazy='selectin', backref="concept", uselist=False)
WorkConcept.concept = db.relationship("Concept", lazy='selectin', backref="work_concept", uselist=False)
WorkTopic.topic = db.relationship("Topic", lazy='selectin', backref="work_topic", uselist=False)
WorkKeyword.keyword = db.relationship("Keyword", lazy='selectin', backref="work_keyword", uselist=False)

Country.continent = db.relationship("Continent", lazy='selectin', backref="countries", uselist=False)

Author.counts = db.relationship("AuthorCounts", lazy='selectin', viewonly=True, uselist=False)
Author.counts_2year = db.relationship("AuthorCounts2Year", lazy='selectin', viewonly=True, uselist=False)
Concept.counts = db.relationship("ConceptCounts", lazy='selectin', viewonly=True, uselist=False)
Concept.counts_2year = db.relationship("ConceptCounts2Year", lazy='selectin', viewonly=True, uselist=False)
Institution.counts = db.relationship("InstitutionCounts", lazy='selectin', viewonly=True, uselist=False)
Institution.counts_2year = db.relationship("InstitutionCounts2Year", lazy='selectin', viewonly=True, uselist=False)
Source.counts = db.relationship("SourceCounts", lazy='selectin', viewonly=True, uselist=False)
Source.counts_2year = db.relationship("SourceCounts2Year", lazy='selectin', viewonly=True, uselist=False)
Work.counts = db.relationship("WorkCounts", lazy='selectin', viewonly=True, uselist=False)
Work.citation_count_2year = db.relationship("Work2YearCitationCount", lazy='selectin', viewonly=True, uselist=False)
Publisher.counts = db.relationship("PublisherCounts", lazy='selectin', viewonly=True, uselist=False)
Publisher.counts_2year = db.relationship("PublisherCounts2Year", lazy='selectin', viewonly=True, uselist=False)
Funder.counts = db.relationship("FunderCounts", lazy='selectin', viewonly=True, uselist=False)
Funder.counts_2year = db.relationship("FunderCounts2Year", lazy='selectin', viewonly=True, uselist=False)
Topic.counts = db.relationship("TopicCounts", lazy='selectin', viewonly=True, uselist=False)
Subfield.counts = db.relationship("SubfieldCounts", lazy='selectin', viewonly=True, uselist=False)
Field.counts = db.relationship("FieldCounts", lazy='selectin', viewonly=True, uselist=False)
Domain.counts = db.relationship("DomainCounts", lazy='selectin', viewonly=True, uselist=False)

Author.counts_by_year_papers = db.relationship("AuthorCountsByYearPapers", lazy='selectin', viewonly=True)
Author.counts_by_year_oa_papers = db.relationship("AuthorCountsByYearOAPapers", lazy='selectin', viewonly=True)
Author.counts_by_year_citations = db.relationship("AuthorCountsByYearCitations", lazy='selectin', viewonly=True)
Concept.counts_by_year = db.relationship("ConceptCountsByYear", lazy='selectin', viewonly=True)
Institution.counts_by_year_papers = db.relationship("InstitutionCountsByYearPapers", lazy='selectin', viewonly=True)
Institution.counts_by_year_oa_papers = db.relationship("InstitutionCountsByYearOAPapers", lazy='selectin', viewonly=True)
Institution.counts_by_year_citations = db.relationship("InstitutionCountsByYearCitations", lazy='selectin', viewonly=True)
Source.counts_by_year_papers = db.relationship("SourceCountsByYearPapers", lazy='selectin', viewonly=True)
Source.counts_by_year_oa_papers = db.relationship("SourceCountsByYearOAPapers", lazy='selectin', viewonly=True)
Source.counts_by_year_citations = db.relationship("SourceCountsByYearCitations", lazy='selectin', viewonly=True)
Work.counts_by_year = db.relationship("WorkCountsByYear", lazy='selectin', viewonly=True)
Publisher.counts_by_year_papers = db.relationship("PublisherCountsByYearPapers", lazy='selectin', viewonly=True)
Publisher.counts_by_year_oa_papers = db.relationship("PublisherCountsByYearOAPapers", lazy='selectin', viewonly=True)
Publisher.counts_by_year_citations = db.relationship("PublisherCountsByYearCitations", lazy='selectin', viewonly=True)
Funder.counts_by_year_papers = db.relationship("FunderCountsByYearPapers", lazy='selectin', viewonly=True)
Funder.counts_by_year_oa_papers = db.relationship("FunderCountsByYearOAPapers", lazy='selectin', viewonly=True)
Funder.counts_by_year_citations = db.relationship("FunderCountsByYearCitations", lazy='selectin', viewonly=True)

Publisher.parent = db.relationship("Publisher", remote_side=[Publisher.publisher_id], lazy='selectin', viewonly=True, uselist=False)
Publisher.self_and_ancestors = db.relationship("PublisherSelfAndAncestors", uselist=True, lazy='selectin', viewonly=True)
Publisher.sources_count = db.relationship("PublisherSources", uselist=False, lazy='selectin', viewonly=True)


# TODO: rename Source.publisher to Source.publisher_name to free up Source.publisher for this relationship
Source.publisher_entity = db.relationship("Publisher", lazy='selectin', viewonly=True, uselist=False)
Source.institution = db.relationship("Institution", lazy='selectin', viewonly=True, uselist=False)

Source.language_override = db.relationship("SourceLanguageOverride", lazy='selectin', uselist=False)

Institution.ancestors = db.relationship("InstitutionAncestors", uselist=True, lazy='selectin', viewonly=True)
Institution.repositories = db.relationship(
    "Source", lazy='selectin', viewonly=True, uselist=True,
    primaryjoin="foreign(Institution.affiliation_id) == remote(Source.institution_id)"
)

Work.safety_journals = db.relationship(
    "Source", lazy="selectin", uselist=True, viewonly=True,
    primaryjoin="remote(Source.display_name) == foreign(Work.original_venue)"
)

# join based on any issn so that we can merge journals and change issn_l without needing to be in sync with recordthresher
Record.journals = db.relationship(
    "Source",
    lazy='selectin',
    uselist=True,  # needs to be a list for now because some duplicate issn_ls in mid.journal still alas
    viewonly=True,
    primaryjoin=or_(
        func.string_to_array(foreign(Record.journal_issn_l), '').bool_op("<@")(remote(Source.issns_text_array)),
        and_(
            foreign(Record.journal_issn_l).is_(None),
            func.json_to_text_array(foreign(Record.journal_issns)).bool_op('&&')(remote(Source.issns_text_array)),
            ~foreign(Record.genre).ilike('%book%')
        ),
        and_(
            foreign(Record.journal_issn_l).is_(None),
            foreign(Record.repository_id) == remote(Source.repository_id)
        ),
        and_(
            foreign(Record.journal_issn_l).is_(None),
            foreign(Record.genre).ilike('%book%'),
            foreign(Record.normalized_book_publisher) == remote(Source.normalized_book_publisher)
        ),
        and_(
            foreign(Record.journal_issn_l).is_(None),
            or_(foreign(Record.genre).ilike('%conference%'), foreign(Record.genre).ilike('%proceeding%')),
            foreign(Record.normalized_conference) == remote(Source.normalized_conference)
        ),
        and_(
            foreign(Record.journal_issn_l).is_(None),
            func.normalize_title(foreign(Record.venue_name)) == remote(Source.normalized_name)
        )
    )
)

Record.parseland_record = db.relationship(
    "Record",
    lazy='selectin',
    uselist=False,
    viewonly=True,
    primaryjoin=and_(foreign(Record.record_type) == 'crossref_doi',
                     remote(Record.record_type) == 'crossref_parseland',
                     foreign(Record.doi) == remote(Record.doi))
)

Record.pdf_record = db.relationship(
    "Record",
    lazy='selectin',
    uselist=False,
    viewonly=True,
    primaryjoin=and_(
        foreign(Record.record_type).in_(['crossref_doi', 'datacite_doi']),
        remote(Record.record_type) == 'parsed_pdf',
        foreign(Record.doi) == remote(Record.doi))
)

Record.hal_records = db.relationship(
    "Record",
    lazy='selectin',
    uselist=True,
    viewonly=True,
    primaryjoin=and_(
        foreign(Record.record_type) == 'crossref_doi',
        foreign(Record.doi) == remote(Record.doi),
        func.lower(remote(Record.pmh_id)).contains('oai:hal')
    )
)

Record.mag_record = db.relationship(
    "Record",
    lazy='selectin',
    uselist=False,
    viewonly=True,
    primaryjoin=and_(
        foreign(Record.record_type) == 'crossref_doi',
        remote(Record.record_type) == 'mag_location',
        foreign(Record.work_id) > 0,
        foreign(Record.work_id) == remote(Record.work_id),
    )
)


Record.legacy_records = db.relationship(
    "Record",
    lazy='selectin',
    uselist=True,
    viewonly=True,
    primaryjoin=and_(
        foreign(Record.record_type) == 'crossref_doi',
        remote(Record.record_type).like('legacy_%'),
        foreign(Record.work_id) > 0,
        foreign(Record.work_id) == remote(Record.work_id),
    )
)



Record.child_records = db.relationship(
    'Record',
    lazy='subquery',
    viewonly=True,
    uselist=True,
    secondary="ins.recordthresher_parent_record",
    primaryjoin="Record.id == RecordthresherParentRecord.parent_record_id",
    secondaryjoin="and_(RecordthresherParentRecord.record_id == Record.id, Record.record_type == 'secondary_pmh_record')"
)

Source.merged_into_source = db.relationship(
    "Source",
    lazy='selectin',
    uselist=False,
    viewonly=True,
    primaryjoin='foreign(Source.merge_into_id) == remote(Source.journal_id)'
)

Record.unpaywall = db.relationship("Unpaywall", lazy='selectin', uselist=False)

Record.work_matches_by_title = db.relationship(
        'Work',
        lazy='subquery',
        viewonly=True,
        uselist=True,
        # foreign_keys="Work.match_title",
        primaryjoin="and_(func.length(foreign(Record.normalized_title)) > 19, foreign(Record.normalized_title) == remote(Work.unpaywall_normalize_title))",
        order_by='desc(remote(Work.full_updated_date))'
    )

Record.work_matches_by_doi = db.relationship(
        'Work',
        lazy='subquery',
        viewonly=True,
        uselist=True,
        # foreign_keys="Work.doi_lower",
        primaryjoin="and_(foreign(Record.doi) != None, func.lower(foreign(Record.doi)) == remote(Work.doi_lower))"
    )

Record.work_matches_by_pmid = db.relationship(
    'Work',
    lazy='subquery',
    viewonly=True,
    uselist=True,
    secondary="mid.work_extra_ids",
    primaryjoin="and_(Record.pmid == WorkExtraIds.attribute_value, WorkExtraIds.attribute_type == 2)",
    secondaryjoin="Work.paper_id == WorkExtraIds.paper_id"
)

Record.work_matches_by_arxiv_id = db.relationship(
        'Work',
        lazy='subquery',
        viewonly=True,
        uselist=True,
        primaryjoin="and_(foreign(Record.arxiv_id) != None, foreign(Record.arxiv_id) == remote(Work.arxiv_id))"
    )


Record.related_version_dois = db.relationship(
    'RecordRelatedVersion',
    lazy='selectin',
    uselist='true',
    primaryjoin="Record.doi == remote(RecordRelatedVersion.doi)",
)

Location.journal = db.relationship('Source', lazy='subquery', viewonly=True, uselist=False)

Author.impact_factor = db.relationship("AuthorImpactFactor", uselist=False, lazy='selectin', viewonly=True)
Source.impact_factor = db.relationship("SourceImpactFactor", uselist=False, lazy='selectin', viewonly=True)
Publisher.impact_factor = db.relationship("PublisherImpactFactor", uselist=False, lazy='selectin', viewonly=True)
Funder.impact_factor = db.relationship("FunderImpactFactor", uselist=False, lazy='selectin', viewonly=True)
Institution.impact_factor = db.relationship("InstitutionImpactFactor", uselist=False, lazy='selectin', viewonly=True)
Concept.impact_factor = db.relationship("ConceptImpactFactor", uselist=False, lazy='selectin', viewonly=True)

Author.h_index = db.relationship("AuthorHIndex", uselist=False, lazy='selectin', viewonly=True)
Author.h_index_2year = db.relationship("AuthorHIndex2Year", uselist=False, lazy='selectin', viewonly=True)
Source.h_index = db.relationship("SourceHIndex", uselist=False, lazy='selectin', viewonly=True)
Source.h_index_2year = db.relationship("SourceHIndex2Year", uselist=False, lazy='selectin', viewonly=True)
Publisher.h_index = db.relationship("PublisherHIndex", uselist=False, lazy='selectin', viewonly=True)
Publisher.h_index_2year = db.relationship("PublisherHIndex2Year", uselist=False, lazy='selectin', viewonly=True)
Funder.h_index = db.relationship("FunderHIndex", uselist=False, lazy='selectin', viewonly=True)
Funder.h_index_2year = db.relationship("FunderHIndex2Year", uselist=False, lazy='selectin', viewonly=True)
Institution.h_index = db.relationship("InstitutionHIndex", uselist=False, lazy='selectin', viewonly=True)
Institution.h_index_2year = db.relationship("InstitutionHIndex2Year", uselist=False, lazy='selectin', viewonly=True)
Concept.h_index = db.relationship("ConceptHIndex", uselist=False, lazy='selectin', viewonly=True)
Concept.h_index_2year = db.relationship("ConceptHIndex2Year", uselist=False, lazy='selectin', viewonly=True)

Author.i10_index = db.relationship("AuthorI10Index", uselist=False, lazy='selectin', viewonly=True)
Author.i10_index_2year = db.relationship("AuthorI10Index2Year", uselist=False, lazy='selectin', viewonly=True)
Source.i10_index = db.relationship("SourceI10Index", uselist=False, lazy='selectin', viewonly=True)
Source.i10_index_2year = db.relationship("SourceI10Index2Year", uselist=False, lazy='selectin', viewonly=True)
Publisher.i10_index = db.relationship("PublisherI10Index", uselist=False, lazy='selectin', viewonly=True)
Publisher.i10_index_2year = db.relationship("PublisherI10Index2Year", uselist=False, lazy='selectin', viewonly=True)
Funder.i10_index = db.relationship("FunderI10Index", uselist=False, lazy='selectin', viewonly=True)
Funder.i10_index_2year = db.relationship("FunderI10Index2Year", uselist=False, lazy='selectin', viewonly=True)
Institution.i10_index = db.relationship("InstitutionI10Index", uselist=False, lazy='selectin', viewonly=True)
Institution.i10_index_2year = db.relationship("InstitutionI10Index2Year", uselist=False, lazy='selectin', viewonly=True)
Concept.i10_index = db.relationship("ConceptI10Index", uselist=False, lazy='selectin', viewonly=True)
Concept.i10_index_2year = db.relationship("ConceptI10Index2Year", uselist=False, lazy='selectin', viewonly=True)


def author_from_id(author_id):
    return Author.query.filter(Author.author_id==author_id).first()

def openalex_id_from_orcid(orcid):
    author_id = db.session.query(AuthorOrcid.author_id).filter(AuthorOrcid.orcid == orcid).limit(1).scalar()
    return f"A{author_id}" if author_id else None

def concept_from_id(concept_id):
    return Concept.query.filter(Concept.field_of_study_id==concept_id).first()

def topic_from_id(topic_id):
    return Topic.query.filter(Topic.topic_id==topic_id).first()

def concept_from_name(name):
    return Concept.query.filter(Concept.display_name.ilike(f'{name}')).order_by(func.length(Concept.display_name)).first()

def topic_from_name(name):
    return Topic.query.filter(Topic.display_name.ilike(f'{name}')).order_by(func.length(Topic.display_name)).first()

def institution_from_id(institution_id):
    return Institution.query.filter(Institution.affiliation_id==institution_id).first()

def openalex_id_from_ror(ror_id):
    affiliation_id = db.session.query(Institution.affiliation_id).filter(Institution.ror_id==ror_id).order_by(Institution.citation_count.desc()).limit(1).scalar()
    print(f"I{affiliation_id}")
    return f"I{affiliation_id}" if affiliation_id else None

def journal_from_id(journal_id):
    return Source.query.filter(Source.journal_id == journal_id).first()

def openalex_id_from_issn(issn):
    journal_id = db.session.query(Source.journal_id).filter(Source.issns.ilike(f'%{issn}%')).order_by(Source.citation_count.desc()).limit(1).scalar()
    return f"S{journal_id}" if journal_id else None

def openalex_id_from_wikidata(wikidata):
    concept_id = db.session.query(Concept.field_of_study_id).filter(Concept.wikidata_id.ilike(f'%{wikidata}')).limit(1).scalar()
    return f"C{concept_id}" if concept_id else None

def record_from_id(record_id):
    return Record.query.filter(Record.id==record_id).first()

def short_openalex_id(long_openalex_id):
    return long_openalex_id.replace("https://openalex.org/", "")

def single_work_query():
    return db.session.query(Work).options(
         selectinload(Work.locations),
         selectinload(Work.journal),
         selectinload(Work.references),
         selectinload(Work.mesh),
         selectinload(Work.counts_by_year),
         selectinload(Work.abstract),
         selectinload(Work.extra_ids),
         selectinload(Work.related_works),
         selectinload(Work.affiliations).selectinload(Affiliation.author).selectinload(Author.orcids),
         selectinload(Work.affiliations).selectinload(Affiliation.institution).selectinload(Institution.ror),
         selectinload(Work.concepts).selectinload(WorkConcept.concept),
         selectinload(Work.topics).selectinload(WorkTopic.topic),
         orm.Load(Work).raiseload('*'))

def work_from_id(work_id):
    my_query = single_work_query()
    return my_query.filter(Work.paper_id==work_id).first()

def openalex_id_from_doi(doi):
    paper_id = db.session.query(Work.paper_id).filter(Work.doi_lower == doi).limit(1).scalar()
    return f"W{paper_id}" if paper_id else None

def openalex_id_from_pmid(pmid):
    pmid_attribute_type = 2
    paper_id = db.session.query(WorkExtraIds.paper_id).filter(WorkExtraIds.attribute_type==pmid_attribute_type, WorkExtraIds.attribute_value==pmid).limit(1).scalar()
    return f"W{paper_id}" if paper_id else None

def hydrate_role(openalex_id_short):
    # for entities that are organizations that can have multiple roles
    # this takes a short ID of one of the roles (e.g., https://openalex.org/I32971472)
    # and adds some known info about the entity
    from models.institution import DELETED_INSTITUTION_ID
    if openalex_id_short.startswith('I'):
        cls = Institution
        role = 'institution'
    elif openalex_id_short.startswith('P'):
        cls = Publisher
        role = 'publisher'
    elif openalex_id_short.startswith('F'):
        cls = Funder
        role = 'funder'
    entity_id = int(openalex_id_short[1:])
    if entity_id == DELETED_INSTITUTION_ID:
        # this institution has been deleted
        return None
    entity = cls.query.options(selectinload(cls.counts).raiseload('*'),
                               orm.Load(cls).raiseload('*')).get(entity_id)
    works_count = int(entity.counts.paper_count or 0) if entity.counts else 0
    return {
        'role': role,
        'id': entity.openalex_id,
        'works_count': works_count,
    }
