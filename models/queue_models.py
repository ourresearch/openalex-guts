from app import db


class QueueWorks(db.Model):
    __table_args__ = {'schema': 'queue'}
    __tablename__ = "work_store"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)

class QueueAuthors(db.Model):
    __table_args__ = {'schema': 'queue'}
    __tablename__ = "author_store"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)

class QueueConcepts(db.Model):
    __table_args__ = {'schema': 'queue'}
    __tablename__ = "concept_store"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)

class QueueInstitutions(db.Model):
    __table_args__ = {'schema': 'queue'}
    __tablename__ = "institution_store"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)

class QueueVenues(db.Model):
    __table_args__ = {'schema': 'queue'}
    __tablename__ = "venue_store"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
