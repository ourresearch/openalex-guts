from app import db

class Domain(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "domains"

    domain_id = db.Column(db.Integer, db.ForeignKey("mid.topics.domain_id"), primary_key=True)
    display_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = {'id': self.domain_id, 
                    'display_name': self.display_name}
        return response

    def __repr__(self):
        return "<Domain ( {} ) {}>".format(self.domain_id, self.display_name)
