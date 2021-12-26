from cached_property import cached_property

from app import db
from models.concept import Concept

class ConceptMetadata(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept_metadata"

    field_of_study_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    wikipedia_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    wikipedia_json = db.Column(db.Text)
    wikidata_json = db.Column(db.Text)
    updated = db.Column(db.DateTime)

    @cached_property
    def display_wikidata_id(self):
        if self.wikidata_id == "'None'":
            return None
        return self.wikidata_id

    @cached_property
    def short_wikidata_id(self):
        if not self.wikidata_id:
            return None
        return self.wikidata_id.replace("https://www.wikidata.org/wiki/", "")

    def __repr__(self):
        return "<ConceptMetadata ( {} ) {} >".format(self.field_of_study_id, self.wikidata_id)

