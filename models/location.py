from app import db


class Location(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "location"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "location"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    source_url = db.Column(db.Text, primary_key=True)
    source_type = db.Column(db.Numeric)
    language_code = db.Column(db.Text)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Location ( {} ) {}>".format(self.paper_id, self.source_url)


