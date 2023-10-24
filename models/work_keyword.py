from app import db


class WorkKeyword(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_keywords"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    keywords = db.Column(db.JSON)
    created = db.Column(db.DateTime)
    updated = db.Column(db.DateTime)
