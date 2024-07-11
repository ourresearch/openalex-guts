from app import db


class WorkAuthorOrcid(db.Model):
    __table_args__ = {'schema': 'orcid'}
    __tablename__ = "final_orcid"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.affiliation.paper_id"), primary_key=True)
    author_sequence_number = db.Column(db.BigInteger, db.ForeignKey("mid.affiliation.author_sequence_number"), primary_key=True)
    orcid = db.Column(db.Text) # shouldn't have more than one but might?
    evidence = db.Column(db.Text)

    @property
    def orcid_url(self):
        if not self.orcid:
            return None
        return "https://orcid.org/{}".format(self.orcid)

    def to_dict(self, return_level="full"):
        if return_level=="full":
            return {'paper_id': self.paper_id,
                    'author_sequence_number': self.author_sequence_number,
                    'orcid': self.orcid_url, 
                    'evidence': self.evidence}
        return {'orcid': self.orcid_url, 'evidence': self.evidence}

    def __repr__(self):
        return "<WorkAuthorOrcid ( {} ) {}>".format(self.paper_id, self.author_sequence_number, self.orcid)