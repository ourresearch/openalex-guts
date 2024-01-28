import datetime
from app import db

class Domain(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "domains"

    domain_id = db.Column(db.Integer, db.ForeignKey("mid.topics.domain_id"), primary_key=True)
    display_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = {'id': self.domain_id, 
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
        return "<Domain ( {} ) {}>".format(self.domain_id, self.display_name)
