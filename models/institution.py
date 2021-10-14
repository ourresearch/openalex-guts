from app import db


class Institution(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_main_affiliations"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "institution"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    official_page = db.Column(db.Text)
    wiki_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    # paper_count integer,
    # paper_family_count integer,
    # citation_count integer,
    # latitude real,
    # longitude real,

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Institution ( {} ) {}>".format(self.affiliation_id, self.display_name)


