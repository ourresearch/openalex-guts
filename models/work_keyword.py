from app import db


class WorkKeyword(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_keyword"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    keyword_id = db.Column(db.Text, db.ForeignKey("mid.keyword.keyword_id"), primary_key=True)
    score = db.Column(db.Float)
    algorithm_version = db.Column(db.Integer)
    updated = db.Column(db.DateTime)
    keywords_input_hash = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        response = self.keyword.to_dict(return_level)
        response["score"] = self.score
        return response
