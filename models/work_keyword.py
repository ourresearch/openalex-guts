from app import db


class WorkKeyword(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_keyword"

    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    keyword_id = db.Column(db.text)
    score = db.Column(db.Float)
    algorithm_version = db.Column(db.Integer)
    updated = db.Column(db.DateTime)
    keywords_input_hash = db.Column(db.Text)
