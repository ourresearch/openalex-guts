from app import db
from models.institution import DELETED_INSTITUTION_ID


class Affiliation(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "affiliation"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"))
    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"))
    author_sequence_number = db.Column(db.Numeric, primary_key=True)
    affiliation_sequence_number = db.Column(db.Numeric, primary_key=True)
    is_corresponding_author = db.Column(db.Boolean)
    original_author = db.Column(db.Text)
    original_affiliation = db.Column(db.Text)
    original_orcid = db.Column(db.Text)
    match_author = db.Column(db.Text)
    match_institution_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)

    def update(self):
        pass

    def to_dict(self, return_level="full"):
        response = {}

        # author_position set in works
        if hasattr(self, "author_position"):
            response["author_position"] = self.author_position

        # keep in this order so author_position is at the top
        response.update({"author": {}, "institution": {}})

        if self.original_author:
            response["author"] = {"id": None, "display_name": self.original_author, "orcid": None}
        if self.original_affiliation:
            response["institution"] = {"id": None, "display_name": self.original_affiliation, "ror": None, "country_code": None, "type": None}

        # overwrite display name with better ones from these dicts if we have them
        if self.author:
            response["author"].update(self.author.to_dict(return_level="minimum"))
        if self.institution and self.institution.affiliation_id != DELETED_INSTITUTION_ID:
            response["institution"].update(self.institution.to_dict(return_level="minimum"))

        response["author_sequence_number"] = self.author_sequence_number
        response["raw_author_name"] = self.original_author
        response["raw_affiliation_string"] = self.original_affiliation
        response["is_corresponding_author"] = self.is_corresponding_author or False

        return response

    def __repr__(self):
        return "<Affiliation ( {} ) {} {}>".format(self.paper_id, self.author_id, self.affiliation_id)
