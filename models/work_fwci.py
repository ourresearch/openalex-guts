from app import db


class WorkFWCI(db.Model):
    __table_args__ = {'schema': 'counts'}
    __tablename__ = "work_fwci"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    fwci = db.Column(db.Float)
    update_date = db.Column(db.DateTime)
