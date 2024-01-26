from app import db

class Field(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "fields"

    field_id = db.Column(db.Integer, db.ForeignKey("mid.topics.field_id"), primary_key=True)
    display_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = {'id': self.field_id, 
                    'display_name': self.display_name}
        return response

    def __repr__(self):
        return "<Field ( {} ) {}>".format(self.field_id, self.display_name)
