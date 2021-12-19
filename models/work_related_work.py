from app import db

class WorkRelatedWork(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_advanced_paper_recommendations"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    recommended_paper_id = db.Column(db.BigInteger, primary_key=True)

    def __repr__(self):
        return "<WorkRelatedWork ( {} ) {}>".format(self.paper_id, self.recommended_paper_id)
