from cached_property import cached_property
from sqlalchemy import text
import json

from app import db


# truncate mid.journal
# insert into mid.journal (select * from legacy.mag_main_journals)
# update mid.journal set display_title=replace(display_title, '\\\\/', '/');
# update mid.journal set publisher=replace(publisher, '\t', '') where publisher ~ '\t';

class Venue(db.Model):
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
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)


    def to_dict(self, return_level="full"):
        response = {
            "id": self.journal_id,
            "display_name": self.display_name,
            "issn_l": self.issn
        }
        if return_level == "full":
            response.update({
                "issns": json.loads(self.issns) if self.issns else None,
                "is_oa": self.is_oa,
                "is_in_doaj": self.is_in_doaj,
                "publisher": self.publisher,
                "webpage": self.webpage,
                "works_count": self.paper_count,
                "cited_by_count": self.citation_count,
                "updated_date": self.updated_date.isoformat()[0:10] if self.updated_date else None,
            })
        return response



    def __repr__(self):
        return "<Venue ( {} ) {}>".format(self.id, self.doi, self.pmh_id, self.pmid)


