from app import db


class Concept(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_advanced_fields_of_study"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "concept"

    field_of_study_id = db.Column(db.BigInteger, primary_key=True)
    rank = db.Column(db.Text)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    main_type = db.Column(db.Text)
    level = db.Column(db.Numeric)
    paper_count = db.Column(db.BigInteger)
    paper_family_count = db.Column(db.BigInteger)
    citation_count = db.Column(db.BigInteger)
    create_date = db.Column(db.DateTime)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Concept ( {} ) {}>".format(self.field_of_study_id, self.display_name)


