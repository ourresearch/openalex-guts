from app import db


class WorkConcept(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_concept"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "concept"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    field_of_study = db.Column(db.BigInteger, db.ForeignKey("mid.concept.field_of_study_id"), primary_key=True)
    score = db.Column(db.Float)


    def to_dict(self, return_level="full"):
        response = self.concept.to_dict(return_level)
        response["score"] = self.score
        return response

    def __repr__(self):
        return "<WorkConcept ( {} ) {}>".format(self.paper_id, self.field_of_study)


