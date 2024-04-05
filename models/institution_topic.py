from app import db

from models.topic import as_topic_openalex_id
from models.topic import Topic


class InstitutionTopic(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution_topic_for_api_full_mv"

    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    topic_id = db.Column(db.Integer, db.ForeignKey("mid.topic.topic_id"), primary_key=True)
    topic_count = db.Column(db.Integer)
    topic_share = db.Column(db.Float)

    def to_dict(self, return_level="full"):
        response = Topic.query.get(self.topic_id).to_dict(return_level)
        if return_level == "count":
            response["count"] = self.topic_count
        elif return_level == "share":
            response["value"] = round(self.topic_share, 7)
        return response

    def __repr__(self):
        return "<InstitutionTopic ( {} ) {}>".format(self.affiliation_id, self.topic_id)