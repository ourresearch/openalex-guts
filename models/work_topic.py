from app import db

# Need to add this for mid.work_topic?
# refresh materialized view mid.work_concept_for_api_mv

class WorkTopic(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_topic"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    topic_id = db.Column(db.Integer, db.ForeignKey("mid.topics.topic_id"), primary_key=True)
    score = db.Column(db.Float)
    algorithm_version = db.Column(db.Integer)
    updated_date = db.Column(db.DateTime)

    def to_dict(self, return_level="full"):
        response = self.topic.to_dict(return_level)
        response["score"] = self.score
        return response

    def __repr__(self):
        return "<WorkTopic ( {} ) {}>".format(self.paper_id, self.topic_id)
