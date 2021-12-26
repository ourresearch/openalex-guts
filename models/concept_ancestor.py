from app import db
from models.concept import Concept

class ConceptAncestor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept_ancestor"

    id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    ancestor_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    my_concept = db.relationship("Concept", foreign_keys=id, backref="ancestors")
    my_ancestor = db.relationship("Concept", foreign_keys=ancestor_id)


    def __repr__(self):
        return "<ConceptAncestor ( {} ) {} >".format(self.id, self.ancestor_id)

