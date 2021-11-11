from app import db


class Abstract(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "abstract"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    indexed_abstract = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        return {"abstract_inverted": self.indexed_abstract}

    def __repr__(self):
        return "<Abstract ( {} ) {}>".format(self.paper_id, self.indexed_abstract[0:100])


