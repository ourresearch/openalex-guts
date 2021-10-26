from app import db


class Institution(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "institution"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    official_page = db.Column(db.Text)
    wiki_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    # paper_count integer,
    # paper_family_count integer,
    # citation_count integer,
    # latitude real,
    # longitude real,

    @property
    def institution_id(self):
        return self.affiliation_id

    @property
    def institution_display_name(self):
        return self.display_name

    @property
    def country_code(self):
        return self.iso3166_code

    @property
    def city(self):
        if not self.grid_address:
            return None
        return self.grid_address.city

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
        else:
            keys = ["institution_id", "institution_display_name", "grid_id", "city"]
        response = {key: getattr(self, key) for key in keys}
        if self.ror:
            response.update(self.ror.to_dict(return_level))
        return response

    def __repr__(self):
        return "<Institution ( {} ) {}>".format(self.affiliation_id, self.display_name)


