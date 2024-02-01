import datetime

from app import db, logger
import models
from util import entity_md5


class Subfield(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "subfield"

    subfield_id = db.Column(db.Integer, db.ForeignKey("mid.topic.subfield_id"), primary_key=True)
    display_name = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    def field(self):
        field_query = (
            db.session.query(models.Field)
            .join(models.Topic, models.Field.field_id == models.Topic.field_id)
            .filter(models.Topic.subfield_id == self.subfield_id)
            .first()
        )
        return field_query.to_dict(return_level="minimum")

    def domain(self):
        domain_query = (
            db.session.query(models.Domain)
            .join(models.Topic, models.Domain.domain_id == models.Topic.domain_id)
            .filter(models.Topic.subfield_id == self.subfield_id)
            .first()
        )
        return domain_query.to_dict(return_level="minimum")

    def topics_list(self):
        topics_query = (
            db.session.query(models.Topic)
            .filter(models.Topic.subfield_id == self.subfield_id)
            .all()
        )
        topics_list = [{"id": topic.openalex_id, "display_name": topic.display_name} for topic in topics_query]
        topics_list_sorted = sorted(topics_list, key=lambda x: x['display_name'].lower())
        return topics_list_sorted

    def store(self):
        bulk_actions = []

        my_dict = self.to_dict()
        my_dict['updated'] = my_dict.get('updated_date')
        my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
        my_dict['@version'] = 1
        entity_hash = entity_md5(my_dict)
        old_entity_hash = self.json_entity_hash

        if entity_hash != old_entity_hash:
            logger.info(f"dictionary for {self.subfield_id} new or changed, so save again")
            index_record = {
                "_op_type": "index",
                "_index": "subfields-v1",
                "_id": self.subfield_id,
                "_source": my_dict
            }
            bulk_actions.append(index_record)
        else:
            logger.info(f"dictionary not changed, don't save again {self.subfield_id}")
        self.json_entity_hash = entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {'id': self.subfield_id, 
                    'display_name': self.display_name}
        if return_level == "full":
            response.update({
                "field": self.field(),
                "domain": self.domain(),
                "topics": self.topics_list(),
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "works_api_url": f"https://api.openalex.org/works?filter=topics.subfield.id={self.subfield_id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return "<Subfield ( {} ) {}>".format(self.subfield_id, self.display_name)
