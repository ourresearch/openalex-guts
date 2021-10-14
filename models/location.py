from app import db


class Location(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_main_paper_urls"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "location"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_papers.paper_id"), primary_key=True)
    source_url = db.Column(db.Text, primary_key=True)
    source_type = db.Column(db.Numeric)
    language_code = db.Column(db.Text)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Location ( {} ) {}>".format(self.paper_id, self.source_url)


