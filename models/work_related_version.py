from app import db


class WorkRelatedVersion(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_related_version"

    id = db.Column(db.Integer, primary_key=True)
    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"))
    version_work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"))
