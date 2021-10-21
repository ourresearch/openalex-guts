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

    @property
    def author_display_name(self):
        return self.display_name

    @property
    def orcid(self):
        return None


    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
        else:
            keys = ["author_id", "author_display_name", "orcid"]
        return {key: getattr(self, key) for key in keys}

    def __repr__(self):
        return "<Author ( {} ) {}>".format(self.author_id, self.display_name)


