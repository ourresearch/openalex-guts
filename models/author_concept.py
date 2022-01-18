from cached_property import cached_property
from app import db
from models.concept import as_concept_openalex_id

# refresh materialized view mid.author_concept_for_api_mv (1200 seconds)

class AuthorConcept(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author_concept_for_api_mv"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    field_of_study_id = db.Column(db.BigInteger, primary_key=True)
    wikidata = db.Column(db.Text)
    display_name = db.Column(db.Text)
    level = db.Column(db.Numeric)
    score = db.Column(db.Float)

    def to_dict(self, return_level="full"):
        response = {
            "id": as_concept_openalex_id(self.field_of_study_id),
            "wikidata": self.wikidata,
            "display_name": self.display_name,
            "level": self.level,
            "score": self.score
        }
        return response

    def __repr__(self):
        return "<AuthorConcept ( {} ) {}>".format(self.author_id, self.field_of_study_id)
