from app import db



class Ror(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "ror_summary"

    ror_id = db.Column(db.Text, db.ForeignKey("mid.institution.ror_id"), primary_key=True)
    name = db.Column(db.Text)
    city = db.Column(db.Text)
    state = db.Column(db.Text)
    country = db.Column(db.Text)
    country_code = db.Column(db.Text)
    grid_id = db.Column(db.Text)


    @property
    def ror_url(self):
        return "https://ror.org/{}".format(self.ror_id)

    def to_dict(self, return_level="full"):
        response = {}
        if hasattr(self, "institution_id"):
            response.update({"institution_id": None,
                             "official_url": None,
                             "wikipedia_url": None,
                             "created_date": None
                             })
        response.update({
            "ror_id": self.ror_id,
            "ror_url": self.ror_url,
            "display_name": self.name,
            "grid_id": self.grid_id,
            "city": self.city,
            "state": self.state,
            "country_code": self.country_code,
            "country": self.country,
        })
        return response

    @classmethod
    def to_dict_null(self):
        response = {
            "ror_id": None,
            "ror_url": None,
            # "display_name": None, overrride with what is in institution
            "grid_id": None,
            "city": None,
            # "country_code": None, overrride with what is in institution
            "country": None,
        }
        return response

    def __repr__(self):
        return "<Ror ( {} ) {}>".format(self.ror_id, self.name)

