import json

from cached_property import cached_property

from app import db


class ISSNtoISSNL(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "journal_issn_to_issnl"

    issn = db.Column(db.Text, primary_key=True)
    issnl = db.Column(db.Text)
    note = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)

    def __repr__(self):
        return f"""<ISSNtoISSNL ISSN: {self.issn} - ISSN-L: {self.issnl}>"""
