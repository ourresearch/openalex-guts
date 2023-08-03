import json

from app import db

class LocationTest(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "location_test"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    location_sequence_number = db.Column(db.Numeric, primary_key=True)
    source_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"))
    landing_page_url = db.Column(db.Text)
    pdf_url = db.Column(db.Text)
    is_oa = db.Column(db.Boolean)
    version = db.Column(db.Text)
    license = db.Column(db.Text)
    doi = db.Column(db.Text)  # it is possible for any location to have its own doi
    endpoint_id = db.Column(db.Text, db.ForeignKey("mid.journal.repository_id"))
    pmh_id = db.Column(db.Text)
    evidence = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)

    @property
    def display_license(self):
        if not self.license:
            return None
        return self.license.lower().split(":", 1)[0]

    @property
    def doi_url(self):
        if not self.doi:
            return None
        return "https://doi.org/{}".format(self.doi.lower())

    def to_dict(self):
        return {
            'source': self.journal and self.journal.to_dict(return_level='minimum'),
            'pdf_url': self.pdf_url,
            'landing_page_url': self.landing_page_url,
            'is_oa': self.is_oa,
            'version': self.version,
            'license': self.display_license,
            'doi': self.doi_url,
        }

    def __repr__(self):
        return "<LocationTest ( {} ) {}>".format(self.paper_id, self.location_sequence_number)


