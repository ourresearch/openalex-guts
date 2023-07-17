from app import db


class WorkSDG(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_sdg"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    predictions = db.Column(db.JSON)
