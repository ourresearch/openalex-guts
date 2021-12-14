import json

from app import db


class Journalsdb(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "journalsdb_flat"

    issn = db.Column(db.Text, db.ForeignKey("mid.journal.issn"), primary_key=True)
    issn_l = db.Column(db.Text)
    issns_string = db.Column(db.Text)
    title = db.Column(db.Text)
    publisher = db.Column(db.Text)

    @property
    def all_issns(self):
        return json.loads(self.issns_string)


    def to_dict(self, return_level="full"):
        response = {}
        if hasattr(self, "journal_id"):
            response.update({"id": None,
                             "display_name": None,
                             "homepage_url": None,
                             "works_count": None,
                             "cited_by_count": None,
                             "updated_date": None
                             })
        response.update({
            "issn_l": self.issn_l,
            "all_issns": self.all_issns,
            "display_name": self.title,
            "publisher": self.publisher,
        })
        return response

    def __repr__(self):
        return "<Journalsdb ( {} ) '{}' {}>".format(self.issn_l, self.title, self.publisher)


