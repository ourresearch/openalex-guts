from app import db


class DOIRegistrationAgency(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "doi_registration_agency"

    doi = db.Column(db.Text, db.ForeignKey("mid.work.doi_lower"), primary_key=True)
    agency = db.Column(db.Text)

    def __repr__(self):
        return "<DOIRegistrationAgency ( {} ) {}>".format(self.doi, self.agency)
