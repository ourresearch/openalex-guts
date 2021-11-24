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
from models.unpaywall import Unpaywall
from models.work_extra_ids import WorkExtraIds


# relationships without association tables
Work.records = db.relationship("Record", lazy='selectin', backref="work")
Work.mesh = db.relationship("Mesh", lazy='selectin', backref="work")
Work.citations = db.relationship("Citation", lazy='selectin', backref="work")
Work.locations = db.relationship("Location", lazy='selectin', backref="work")
Work.abstract = db.relationship("Abstract", lazy='selectin', backref="work", uselist=False)
Work.journal = db.relationship("Journal", lazy='selectin', backref="work", uselist=False)
Work.unpaywall = db.relationship("Unpaywall", lazy='selectin', backref="work", uselist=False)
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

def work_from_id(work_id):
    return Work.query.filter(Work.paper_id==work_id).first()

def work_from_doi(doi):
    return Work.query.filter(Work.doi_lower==doi).first()

def work_from_pmid(pmid):
    return Work.query.filter(Work.pmid==pmid).first()
