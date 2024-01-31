import datetime

from cached_property import cached_property
from sqlalchemy import orm

from app import db
from app import get_apiurl_from_openalex_url
from app import logger
from util import entity_md5


def as_topic_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/T{id}"


class Topic(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "topic"

    topic_id = db.Column(db.Integer, primary_key=True)
    display_name = db.Column(db.Text)
    summary = db.Column(db.Text)
    keywords = db.Column(db.Text)
    subfield_id = db.Column(db.Integer)
    field_id = db.Column(db.Integer)
    domain_id = db.Column(db.Integer)
    json_entity_hash = db.Column(db.Text)
    wikipedia_url = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.topic_id

    @property
    def openalex_id(self):
        return as_topic_openalex_id(self.topic_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    def store(self):
        bulk_actions = []

        my_dict = self.to_dict()
        my_dict['updated'] = my_dict.get('updated_date')
        my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
        my_dict['@version'] = 1
        entity_hash = entity_md5(my_dict)
        old_entity_hash = self.json_entity_hash

        if entity_hash != old_entity_hash:
            logger.info(f"dictionary for {self.openalex_id} new or changed, so save again")
            index_record = {
                "_op_type": "index",
                "_index": "topics-v1",
                "_id": self.openalex_id,
                "_source": my_dict
            }
            bulk_actions.append(index_record)
        else:
            logger.info(f"dictionary not changed, don't save again {self.openalex_id}")
        self.json_entity_hash = entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
            "subfield": self.subfield.to_dict("minimal"),
            "field": self.field.to_dict("minimal"),
            "domain": self.domain.to_dict("minimal")
        }
        if return_level == "full":
            response.update({
                "description": self.summary,
                "keywords": [keyword.strip() for keyword in self.keywords.split(";")] if self.keywords else [],
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "wikipedia_url": self.wikipedia_url,
                "works_api_url": f"https://api.openalex.org/works?filter=topics.id:{self.openalex_id_short}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return "<Topic ( {} ) {} {}>".format(self.openalex_api_url, self.id, self.display_name)


logger.info(f"loading valid topic IDs")
_valid_topics = db.session.query(Topic.topic_id).options(orm.Load(Topic).raiseload('*')).all()
_valid_topic_ids = set([t.topic_id for t in _valid_topics])


def is_valid_topic_id(topic_id):
    return topic_id and topic_id in _valid_topic_ids
