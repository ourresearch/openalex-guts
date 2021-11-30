from app import db

# alter table institution rename column normalized_name to mag_normalized_name
# alter table institution add column normalized_name varchar(65000)
# update institution set normalized_name=f_normalize_title(institution.mag_normalized_name)

# truncate mid.institution
# insert into mid.institution (select * from legacy.mag_main_affiliations)
# update mid.institution set display_name=replace(display_name, '\t', '') where display_name ~ '\t';

class Institution(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    official_page = db.Column(db.Text)
    wiki_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    ror_id = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    # latitude real,
    # longitude real,

    @property
    def institution_id(self):
        return self.affiliation_id

    @property
    def institution_display_name(self):
        if self.ror:
            return self.ror.name
        return self.display_name

    @property
    def ror_url(self):
        return "https://ror.org/{}".format(self.ror_id)

    @property
    def country_code(self):
        if not self.iso3166_code:
            return None
        return self.iso3166_code.lower()

    @classmethod
    def to_dict_null(self):
        response = {
            "institution_id": self.institution_id,
            "display_name": self.display_name,
            "ror_id": None,
            "country_code": self.country_code,
            "official_page": self.official_page,
            "wiki_page": self.wiki_page,
            "paper_count": None,
            "citation_count": None,
            "created_date": self.created_date
        }
        return response

    def to_dict(self, return_level="full"):
        from models import Ror

        response = {
            "institution_id": self.institution_id,
            "display_name": self.display_name,
            "ror_id": None,
        }
        if self.ror:
            response.update(self.ror.to_dict(return_level))
        else:
            response.update(Ror.to_dict_null())

        response.update({
            "country_code": self.country_code,
            "official_page": self.official_page,
            "wiki_page": self.wiki_page,
            "paper_count": self.paper_count,
            "citation_count": self.citation_count,
            "created_date": self.created_date
        })
        return response

    def __repr__(self):
        return "<Institution ( {} ) {}>".format(self.affiliation_id, self.display_name)


