from cached_property import cached_property
from sqlalchemy import text
import json

from app import db


# truncate mid.journal
# insert into mid.journal (select * from legacy.mag_main_journals)
# update mid.journal set display_title=replace(display_title, '\\\\/', '/');
# update mid.journal set publisher=replace(publisher, '\t', '') where publisher ~ '\t';

class Journal(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "journal"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.journal_id"), primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    issn = db.Column(db.Text)
    issns = db.Column(db.Text)
    is_oa = db.Column(db.Boolean)
    is_in_doaj = db.Column(db.Boolean)
    publisher = db.Column(db.Text)
    webpage = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def paper_count(self):
        q = """select count(distinct paper_id) as n
            from mid.work work
            where journal_id = :journal_id;"""
        row = db.session.execute(text(q), {"journal_id": self.journal_id}).first()
        return row[0]

    @cached_property
    def citation_count(self):
        q = """select count(distinct citation.paper_id) as n
            from mid.work work
            join mid.citation citation on work.paper_id=citation.paper_reference_id
            where journal_id = :journal_id;"""
        row = db.session.execute(text(q), {"journal_id": self.journal_id}).first()
        return row[0]

    def to_dict(self, return_level="full"):
        response = {
            "journal_id": self.journal_id,
            "display_name": self.display_name,
            "issn_l": self.issn,
            "issns": json.loads(self.issns) if self.issns else None,
            "is_oa": self.is_oa,
            "is_in_doaj": self.is_in_doaj,
            "publisher": self.publisher,
            "webpage": self.webpage,
            "paper_count": self.paper_count,   # NO_CITATIONS_FOR_NOW
            "citation_count": self.citation_count,   # NO_CITATIONS_FOR_NOW
            "created_date": self.created_date,
            "updated_date": self.updated_date.isoformat()[0:10] if self.updated_date else None,
        }
        return response



    def __repr__(self):
        return "<Journal ( {} ) {}>".format(self.id, self.doi, self.pmh_id, self.pmid)


