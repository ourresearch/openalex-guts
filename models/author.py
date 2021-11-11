from app import db


# truncate mid.author
# insert into mid.author (select * from legacy.mag_main_authors)

class Author(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author"

    author_id = db.Column(db.BigInteger, primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    last_known_affiliation_id = db.Column(db.Numeric)
    paper_count = db.Column(db.Numeric)
    # paper_family_count integer,
    match_name = db.Column(db.Text)
    citation_count = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)

    @property
    def author_display_name(self):
        return self.display_name

    @property
    def orcid(self):
        if not self.orcids:
            return None
        return sorted(self.orcids, key=lambda x: x.orcid)[0]

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = ["author_id", "display_name", "match_name", "last_known_affiliation_id", "citation_count"]
        else:
            keys = ["author_id", "author_display_name"]
        response = {key: getattr(self, key) for key in keys}
        if self.orcid:
            response["orcid"] = self.orcid.to_dict(return_level)
        return response

    def __repr__(self):
        return "<Author ( {} ) {}>".format(self.author_id, self.display_name)


