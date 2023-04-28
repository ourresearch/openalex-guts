from app import db


class WorkOpenAPC(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_openapc"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    doi = db.Column(db.Text)
    year = db.Column(db.Integer)
    apc_in_euro = db.Column(db.Integer)
    apc_in_usd = db.Column(db.Integer)
