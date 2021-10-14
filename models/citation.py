from app import db


class Citation(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_main_paper_references_id"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "citation"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_papers.paper_id"), primary_key=True)
    paper_reference_id = db.Column(db.BigInteger, primary_key=True)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Citation ( {} ) {}>".format(self.paper_id, self.paper_reference_id)


