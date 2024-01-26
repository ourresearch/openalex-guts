from app import db

class Subfield(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "subfields"

    subfield_id = db.Column(db.Integer, db.ForeignKey("mid.topics.subfield_id"), primary_key=True)
    display_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = {'id': self.subfield_id, 
                    'display_name': self.display_name}
        return response

    def __repr__(self):
        return "<Subfield ( {} ) {}>".format(self.subfield_id, self.display_name)
