from app import db



class Ror(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "ror_summary_view"

    ror_id = db.Column(db.Text, db.ForeignKey("mid.institution_ror.ror_id"), primary_key=True)
    grid_id = db.Column(db.Text)
    name = db.Column(db.Text)
    city = db.Column(db.Text)
    country = db.Column(db.Text)
    country_code = db.Column(db.Text)


    @property
    def ror_url(self):
        return "https://ror.org/{}".format(self.ror_id)

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
        else:
            keys = ["grid_id", "city", "country", "country_code"]
        response = {key: getattr(self, key) for key in keys}
        return response

    def __repr__(self):
        return "<Ror ( {} ) {}>".format(self.ror_id, self.name)

