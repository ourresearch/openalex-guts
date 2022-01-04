from app import db

# alter table affiliation add column match_name varchar(65000)
# update affiliation set normalized_institution_name=f_matching_string(original_affiliation) where original_affiliation is not null

# alter table crossref_main_author add column normalized_author text
# update crossref_main_author set normalized_author=f_normalize_author(given || ' ' || family) where family is not null and given is not null

# alter table pubmed_main_author add column normalized_author text
# update pubmed_main_author set normalized_author=f_normalize_author(coalesce(given, '') || ' ' || coalesce(initials, '') || ' ' || family) where family is not null

# truncate mid.affiliation
# insert into mid.affiliation (select * from legacy.mag_main_paper_author_affiliations)
# update mid.affiliation set original_author=replace(original_author, '\t', '') where original_author ~ '\t';

class Affiliation(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "affiliation"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    author_sequence_number = db.Column(db.Numeric, primary_key=True)
    original_author = db.Column(db.Text)
    original_affiliation = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        response = {}


        # author_position set in works
        if hasattr(self, "author_position"):
            response["author_position"] = self.author_position
        # keep in this order so author_position is at the top
        response.update({"author": None, "institution": None})

        if self.original_author:
            response["author"] = {"id": self.author_id, "display_name": self.original_author, "orcid": None}
        if self.original_affiliation:
            response["institution"] = {"id": self.affiliation_id, "display_name": self.original_affiliation, "ror": None, "country_code": None, "type": None}

        # overwrite display name with better ones from these dicts if we have them
        if self.author:
            response["author"].update(self.author.to_dict(return_level="minimum"))
        if self.institution:
            response["institution"].update(self.institution.to_dict(return_level="minimum"))

        response["author_sequence_number"] = self.author_sequence_number

        return response



    def __repr__(self):
        return "<Affiliation ( {} ) {} {}>".format(self.paper_id, self.author_id, self.affiliation_id)
