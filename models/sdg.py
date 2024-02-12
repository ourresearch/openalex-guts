import datetime

from cached_property import cached_property
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from requests_cache import CachedSession, RedisCache
from redis import Redis

from app import db
from app import logger
from app import REDIS_URL
from app import SDGS_INDEX, WORKS_INDEX, ELASTIC_URL
from util import entity_md5


class SDG(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "sdg"

    sdg_id = db.Column(db.Integer, primary_key=True)
    display_name = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.sdg_id

    @property
    def cache_expiration(self):
        return 3600 * 24 * 3

    def store(self):
        bulk_actions = []

        my_dict = self.to_dict()
        my_dict['updated'] = my_dict.get('updated_date')
        my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
        entity_hash = entity_md5(my_dict)
        old_entity_hash = self.json_entity_hash

        if entity_hash != old_entity_hash:
            logger.info(f"dictionary for {self.id} new or changed, so save again")
            index_record = {
                "_op_type": "index",
                "_index": SDGS_INDEX,
                "_id": self.id,
                "_source": my_dict
            }
            bulk_actions.append(index_record)
        else:
            logger.info(f"dictionary not changed, don't save again {self.id}")
        self.json_entity_hash = entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.un_metadata_id,
            "display_name": self.display_name,
        }
        if return_level == "full":
            response.update({
                "works_count": self.works_count,
                "citation_count": self.cached_citation_count,
                "works_api_url": f"https://api.openalex.org/works?filter=sustainable_development_goals.id:{self.un_metadata_id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    @property
    def un_metadata_id(self):
        return f"https://metadata.un.org/sdg/{self.sdg_id}"

    @cached_property
    def works_count(self):
        session = self.cached_session()
        r = session.get("https://api.openalex.org/works?group-by=sustainable_development_goals.id")
        group_by = r.json().get("group_by")
        for group in group_by:
            if group.get("key") == self.un_metadata_id:
                return group.get("count")

    @property
    def cached_citation_count(self):
        redis = Redis.from_url(REDIS_URL)
        cache_key = f"sdg_{self.id}_citation_count"

        # try to retrieve the cached value
        cached_citation_count = redis.get(cache_key)
        if cached_citation_count is not None:
            cached_citation_count_str = cached_citation_count.decode('utf-8')
            return int(float(cached_citation_count_str))

        # if not cached, compute the value
        citation_count = self.citation_count()
        logger.info(f"Citation count for {self.id} is {citation_count}")

        # cache the newly computed value
        redis.set(cache_key, citation_count, ex=self.cache_expiration)
        return int(citation_count)

    def citation_count(self):
        es = Elasticsearch([ELASTIC_URL], timeout=30)
        s = Search(using=es, index=WORKS_INDEX)
        s = s.query("term", sustainable_development_goals__id=self.un_metadata_id)
        s.aggs.bucket("sdg_citation_count", "sum", field="cited_by_count")
        response = s.execute()
        return response.aggregations.sdg_citation_count.value

    def cached_session(self):
        connection = Redis.from_url(REDIS_URL)
        cache_backend = RedisCache(connection=connection, expire_after=None)
        session = CachedSession(cache_name="cache", backend=cache_backend,
                                expire_after=self.cache_expiration)
        return session

    def __repr__(self):
        return "<SDG ( {} ) {} {}>".format(self.id, self.un_metadata_id, self.display_name)
