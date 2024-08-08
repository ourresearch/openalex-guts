from app import db


class WorkCitationNormPer(db.Model):
    __table_args__ = {'schema': 'counts'}
    __tablename__ = "work_norm_citation_percentile_by_type_year_subfield"

    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    normalized_citation_percentile = db.Column(db.Float)
    update_date = db.Column(db.DateTime)
