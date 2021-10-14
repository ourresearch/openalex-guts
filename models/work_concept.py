from app import db


class WorkConcept(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_advanced_paper_fields_of_study"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "concept"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_papers.paper_id"), primary_key=True)
    field_of_study = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_advanced_fields_of_study.field_of_study_id"), primary_key=True)
    score = db.Column(db.Float)


    def to_dict(self):
        response = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        response["concept"] = self.concept.to_dict()
        return response

    def __repr__(self):
        return "<WorkConcept ( {} ) {}>".format(self.paper_id, self.field_of_study)


