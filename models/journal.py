from app import db


class Journal(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "journal"

    # __table_args__ = {'schema': 'work'}
    # __tablename__ = "journal"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.journal_id"), primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    issn = db.Column(db.Text)
    publisher = db.Column(db.Text)
    webpage = db.Column(db.Text)
    # paper_count bigint,
    # paper_family_count bigint,
    # citation_count bigint,
    webpage = db.Column(db.DateTime)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Journal ( {} ) {}>".format(self.id, self.doi, self.pmh_id, self.pmid)


