from app import db


class Unpaywall(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "unpaywall"

    doi = db.Column(db.Text, db.ForeignKey("mid.work.doi_lower"), primary_key=True)
    genre = db.Column(db.Text)
    journal_is_oa = db.Column(db.Text)
    oa_status = db.Column(db.Text)
    has_green = db.Column(db.Boolean)
    is_oa_bool = db.Column(db.Boolean)
    best_version = db.Column(db.Text)
    best_license = db.Column(db.Text)
    best_host_type = db.Column(db.Text)
    best_url = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        keys = [col.name for col in self.__table__.columns if col.name not in ["doi"]]
        return {key: getattr(self, key) for key in keys}

    def __repr__(self):
        return "<Unpaywall ( {} ) '{}' {}>".format(self.issn_l, self.title, self.publisher)


