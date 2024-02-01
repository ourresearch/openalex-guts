import datetime

from app import db, logger
import models
from util import entity_md5


class Domain(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "domain"

    domain_id = db.Column(db.Integer, db.ForeignKey("mid.topic.domain_id"), primary_key=True)
    display_name = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    def fields(self):
        fields_query = (
            db.session.query(models.Field)
            .join(models.Topic, models.Field.field_id == models.Topic.field_id)
            .filter(models.Topic.domain_id == self.domain_id)
            .all()
        )
        return [field.to_dict(return_level="minimum") for field in fields_query]

    def store(self):
        bulk_actions = []

        my_dict = self.to_dict()
        my_dict['updated'] = my_dict.get('updated_date')
        my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
        my_dict['@version'] = 1
        entity_hash = entity_md5(my_dict)
        old_entity_hash = self.json_entity_hash

        if entity_hash != old_entity_hash:
            logger.info(f"dictionary for {self.domain_id} new or changed, so save again")
            index_record = {
                "_op_type": "index",
                "_index": "domains-v1",
                "_id": self.domain_id,
                "_source": my_dict
            }
            bulk_actions.append(index_record)
        else:
            logger.info(f"dictionary not changed, don't save again {self.domain_id}")
        self.json_entity_hash = entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {'id': self.domain_id, 
                    'display_name': self.display_name}
        if return_level == "full":
            response.update({
                "fields": self.fields(),
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "works_api_url": f"https://api.openalex.org/works?filter=topics.domain.id:{self.domain_id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return "<Domain ( {} ) {}>".format(self.domain_id, self.display_name)
