from app import db

# alter table institution rename column normalized_name to mag_normalized_name
# alter table institution add column normalized_name varchar(65000)
# update institution set normalized_name=f_normalize_title(institution.mag_normalized_name)

# truncate mid.institution
# insert into mid.institution (select * from legacy.mag_main_affiliations)


class Institution(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution"

    # grid_id mapped to ror and in institution_ror
    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    official_page = db.Column(db.Text)
    wiki_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    # latitude real,
    # longitude real,

    @property
    def institution_id(self):
        return self.affiliation_id

    @property
    def institution_display_name(self):
        if self.institution_ror:
            return self.institution_ror.ror.name
        return self.display_name

    @property
    def country_code(self):
        if not self.iso3166_code:
            return None
        return self.iso3166_code.lower()

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
        else:
            keys = ["institution_id", "institution_display_name", "country_code"]
        response = {key: getattr(self, key) for key in keys}
        if self.institution_ror:
            response.update(self.institution_ror.to_dict(return_level))
        return response

    def __repr__(self):
        return "<Institution ( {} ) {}>".format(self.affiliation_id, self.display_name)


