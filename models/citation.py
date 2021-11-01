from app import db


# truncate mid.citation
# insert into mid.citation (select * from legacy.mag_main_paper_references_id)

class Citation(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    paper_reference_id = db.Column(db.BigInteger, primary_key=True)

    def to_dict(self, return_level="full"):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Citation ( {} ) {}>".format(self.paper_id, self.paper_reference_id)


