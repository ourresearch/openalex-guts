from app import db

class Author(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "author"

    author_id = db.Column(db.BigInteger, primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    last_known_affiliation_id = db.Column(db.Numeric)
    paper_count = db.Column(db.Numeric)
    # paper_family_count integer,
    citation_count = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Author ( {} ) {}>".format(self.author_id, self.display_name)


