from cached_property import cached_property
from app import db


# truncate mid.work_concept
# insert into mid.work_concept (select * from legacy.mag_advanced_paper_fields_of_study)

class WorkConcept(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_concept"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    field_of_study = db.Column(db.BigInteger, db.ForeignKey("mid.concept.field_of_study_id"), primary_key=True)
    score = db.Column(db.Float)

    @cached_property
    def is_valid(self):
        return self.concept.is_valid

    def to_dict(self, return_level="full"):
        response = self.concept.to_dict(return_level)
        response["score"] = self.score
        return response

    def __repr__(self):
        return "<WorkConcept ( {} ) {}>".format(self.paper_id, self.field_of_study)


