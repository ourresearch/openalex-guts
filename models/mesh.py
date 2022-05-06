from app import db


# truncate mid.mesh
# insert into mid.mesh (select * from legacy.mag_advanced_paper_mesh)

class Mesh(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "mesh"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    descriptor_ui = db.Column(db.Text, primary_key=True)
    descriptor_name = db.Column(db.Text)
    qualifier_ui = db.Column(db.Text)
    qualifier_name = db.Column(db.Text)
    is_major_topic = db.Column(db.Boolean)

    def to_dict(self, return_level=None):
        response = {
            "is_major_topic": self.is_major_topic,
            "descriptor_ui": self.descriptor_ui,
            "descriptor_name": self.descriptor_name,
            "qualifier_ui": self.qualifier_ui,
            "qualifier_name": self.qualifier_name,
        }
        return response

    def __repr__(self):
        return "<Mesh ( {} ) {}>".format(self.paper_id, self.descriptor_name)


