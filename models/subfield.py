import datetime
from app import db

class Subfield(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "subfield"

    subfield_id = db.Column(db.Integer, db.ForeignKey("mid.topic.subfield_id"), primary_key=True)
    display_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = {'id': self.subfield_id, 
                    'display_name': self.display_name}
        if return_level == "full":
            response.update({
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                # TODO summary_stats
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return "<Subfield ( {} ) {}>".format(self.subfield_id, self.display_name)
