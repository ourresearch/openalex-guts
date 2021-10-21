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
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
        else:
            keys = ["all_issns", "title", "publisher"]
        return {key: getattr(self, key) for key in keys}

    def __repr__(self):
        return "<Journalsdb ( {} ) '{}' {}>".format(self.issn_l, self.title, self.publisher)


