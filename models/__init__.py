from sqlalchemy.orm import selectinload
from sqlalchemy import orm


from app import db

from models.abstract import Abstract
from models.author import Author
from models.author_orcid import AuthorOrcid
from models.citation import Citation
from models.concept import Concept
from models.institution import Institution
from models.journal import Journal
from models.location import Location
from models.mesh import Mesh
from models.record import Record
from models.work import Work
from models.affiliation import Affiliation
from models.work_concept import WorkConcept
from models.ror import Ror
from models.journalsdb import Journalsdb
from models.work_extra_ids import WorkExtraIds


# relationships without association tables
Work.records = db.relationship("Record", lazy='selectin', backref="work")
Work.mesh = db.relationship("Mesh", lazy='selectin', backref="work")
Work.citations = db.relationship("Citation", lazy='selectin', backref="work")
Work.locations = db.relationship("Location", lazy='selectin', backref="work")
Work.abstract = db.relationship("Abstract", lazy='selectin', backref="work", uselist=False)
Work.journal = db.relationship("Journal", lazy='selectin', backref="work", uselist=False)
Work.extra_ids = db.relationship("WorkExtraIds", lazy='selectin', backref="work")

# relationships with association tables
Work.affiliations = db.relationship("Affiliation", lazy='selectin', backref="work")
Work.concepts = db.relationship("WorkConcept", lazy='selectin', backref="work")

Affiliation.author = db.relationship("Author")
Affiliation.institution = db.relationship("Institution")

Institution.ror = db.relationship("Ror", uselist=False)
Journal.journalsdb = db.relationship("Journalsdb", uselist=False)
Author.orcids = db.relationship("AuthorOrcid", backref="author")
Author.last_known_institution = db.relationship("Institution")

# Concept.works = db.relationship("WorkConcept", lazy='selectin', backref="concept", uselist=False)
WorkConcept.concept = db.relationship("Concept", lazy='selectin', backref="work_concept", uselist=False)


def author_from_id(author_id):
    return Author.query.filter(Author.author_id==author_id).first()

def authors_from_orcid(orcid):
    author_orcids = AuthorOrcid.query.filter(AuthorOrcid.orcid==orcid).all()
    authors = [author_orcid.author for author_orcid in author_orcids]
    return authors

def concept_from_id(concept_id):
    return Concept.query.filter(Concept.concept_id==concept_id).first()

def institution_from_id(institution_id):
    return Institution.query.filter(Institution.affiliation_id==institution_id).first()

def institutions_from_ror(ror_id):
    response = Institution.query.filter(Institution.ror_id==ror_id).all()
    if not response:
        response_ror = Ror.query.filter(Ror.ror_id==ror_id).first()
        response_ror.institution_id = None
        response = [response_ror]
    return response

def journal_from_id(journal_id):
    return Journal.query.filter(Journal.journal_id==journal_id).first()

def journals_from_issn(issn):
    response = Journal.query.filter(Journal.issns.ilike(f'%{issn}%')).all()
    if not response:
        response_journalsdb = Journalsdb.query.filter(Journalsdb.issn==issn).first()
        response_journalsdb.journal_id = None
        response = [response_journalsdb]
    return response

def record_from_id(record_id):
    return Record.query.filter(Record.id==record_id).first()

def single_work_query():
    return db.session.query(Work).options(
         selectinload(Work.locations),
         selectinload(Work.journal).selectinload(Journal.journalsdb),
         selectinload(Work.citations),
         selectinload(Work.mesh),
         selectinload(Work.abstract),
         selectinload(Work.extra_ids),
         selectinload(Work.affiliations).selectinload(Affiliation.author).selectinload(Author.orcids),
         selectinload(Work.affiliations).selectinload(Affiliation.institution).selectinload(Institution.ror),
         selectinload(Work.concepts).selectinload(WorkConcept.concept),
         orm.Load(Work).raiseload('*'))

def work_from_id(work_id):
    my_query = single_work_query()
    return my_query.filter(Work.paper_id==work_id).first()

def work_from_doi(doi):
    my_query = single_work_query()
    return my_query.filter(Work.doi_lower == doi).first()

def work_from_pmid(pmid):
    pmid_attribute_type = 2
    work_extra_id = WorkExtraIds.query.filter(WorkExtraIds.attribute_type==pmid_attribute_type, WorkExtraIds.attribute_value==pmid).first()
    return work_extra_id.work if work_extra_id else None
