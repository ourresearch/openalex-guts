from app import db


class Mesh(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_advanced_paper_mesh"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "mesh"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_papers.paper_id"), primary_key=True)
    descriptor_ui = db.Column(db.Text, primary_key=True)
    descriptor_name = db.Column(db.Text)
    qualifier_ui = db.Column(db.Text, primary_key=True)
    qualifier_name = db.Column(db.Text)
    qualifier_name = db.Column(db.Boolean)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Mesh ( {} ) {}>".format(self.paper_id, self.descriptor_name)

