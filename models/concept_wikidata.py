from cached_property import cached_property

from app import db
from models.concept import Concept

class ConceptWikidata(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "wiki_concept"

    field_of_study_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept.field_of_study_id"), primary_key=True)
    wikipedia_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    wikipedia_json = db.Column(db.Text)
    wikidata_json = db.Column(db.Text)

    @cached_property
    def is_valid(self):
        if self.wikidata_id == "'None'" or not self.wikidata_json:
            return False
        return True

    @cached_property
    def display_wikidata_id(self):
        if self.wikidata_id == "'None'":
            return None
        return self.wikidata_id

    def __repr__(self):
        return "<ConceptWikidata ( {} ) {} >".format(self.field_of_study_id, self.wikidata_id)

