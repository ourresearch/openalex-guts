from app import db

class WorkRelatedWork(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "related_work"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    recommended_paper_id = db.Column(db.BigInteger, primary_key=True)
    score = db.Column(db.Float)
    updated = db.Column(db.DateTime)

    def __repr__(self):
        return "<WorkRelatedWork ( {} ) {}>".format(self.paper_id, self.recommended_paper_id)
