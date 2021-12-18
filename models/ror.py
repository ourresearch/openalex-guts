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
    wikipedia_url = db.Column(db.Text)


    @property
    def ror_url(self):
        return "https://ror.org/{}".format(self.ror_id)

    @property
    def country_code_upper(self):
        if not self.country_code:
            return None
        return self.country_code.upper()

    def __repr__(self):
        return "<Ror ( {} ) {}>".format(self.ror_id, self.name)

