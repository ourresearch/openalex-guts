import datetime

from cached_property import cached_property
from sqlalchemy import orm

from app import db
from app import get_apiurl_from_openalex_url
from app import logger
# from app import KEYWORDS_INDEX
import models
from util import entity_md5


def as_keyword_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/keywords/{id}"


class Keyword(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "keyword"

    keyword_id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.keyword_id

    @property
    def openalex_id(self):
        return as_keyword_openalex_id(self.keyword_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    # @property
    # def siblings(self):
    #     siblings_query = (
    #         db.session.query(models.Topic)
    #         .filter(models.Topic.subfield_id == self.subfield_id)
    #         .all()
    #     )
    #     topics_list = [
    #         {'id': topic.openalex_id, 'display_name': topic.display_name}
    #         for topic in siblings_query
    #         if topic.topic_id != self.topic_id
    #     ]
    #     topics_list_sorted = sorted(topics_list, key=lambda x: x['display_name'].lower())
    #     return topics_list_sorted

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
                # "_index": KEYWORDS_INDEX,
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
            "keyword": self.display_name
        }
        if return_level == "full":
            response.update({
                # "siblings": self.siblings,
                # "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                # "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "works_api_url": f"https://api.openalex.org/works?filter=keywords.id:{self.openalex_id_short}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return "<Keyword ( {} ) {} {}>".format(self.openalex_api_url, self.id, self.display_name)


logger.info(f"loading valid keyword IDs")
_valid_keywords = db.session.query(Keyword.keyword_id).options(orm.Load(Keyword).raiseload('*')).all()
_valid_keyword_ids = set([k.keyword_id for k in _valid_keywords])


def is_valid_keyword_id(keyword_id):
    return keyword_id and keyword_id in _valid_keyword_ids
