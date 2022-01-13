from app import db


class Orcid(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "orcid"

    orcid = db.Column(db.Text, db.ForeignKey("mid.author_orcid.orcid"), primary_key=True)
    api_json = db.Column(db.Text)


    def __repr__(self):
        return "<Orcid ( {} )>".format(self.orcid)


