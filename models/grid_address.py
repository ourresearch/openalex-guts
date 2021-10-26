from app import db


class GridAddress(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "grid_addresses"

    grid_id = db.Column(db.Text, db.ForeignKey("mid.institution.grid_id"), primary_key=True)
    city = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        keys = [col.name for col in self.__table__.columns]
        response = {key: getattr(self, key) for key in keys}
        return response

    def __repr__(self):
        return "<GridAddress ( {} ) {}>".format(self.grid_id, self.city)

