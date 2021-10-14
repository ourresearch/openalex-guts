from app import db

class Affiliation(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_main_paper_author_affiliations"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "concept"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_papers.paper_id"), primary_key=True)
    author_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_authors.author_id"), primary_key=True)
    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_affiliations.affiliation_id"), primary_key=True)
    author_sequence_number = db.Column(db.Numeric, primary_key=True)
    original_author = db.Column(db.Text)
    original_affiliation = db.Column(db.Text)

    def to_dict(self):
        response = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        response["author"] = self.author.to_dict()
        response["institution"] = self.institution.to_dict()
        return response

    def __repr__(self):
        return "<Affiliation ( {} ) {} {}>".format(self.paper_id, self.author_id, self.affiliation_id)


