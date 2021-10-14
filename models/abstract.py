from app import db


class Abstract(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "abstract"

    work_id = db.Column(db.BigInteger, db.ForeignKey("legacy.mag_main_papers.paper_id"), primary_key=True)
    abstract = db.Column(db.Text)

    def to_dict(self):
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}

    def __repr__(self):
        return "<Abstract ( {} ) {}>".format(self.work_id, self.abstract[0:100])

