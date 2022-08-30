from app import db


# truncate mid.work_concept
# insert into mid.work_concept (select * from legacy.mag_advanced_paper_fields_of_study)

# refresh materialized view mid.work_concept_for_api_mv

class WorkConcept(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_concept"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    field_of_study = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    score = db.Column(db.Float)
    algorithm_version = db.Column(db.Integer)
    uses_newest_algorithm = db.Column(db.Boolean)
    updated_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = self.concept.to_dict(return_level)
        response["score"] = self.score
        return response

    def __repr__(self):
        return "<WorkConcept ( {} ) {}>".format(self.paper_id, self.field_of_study)
