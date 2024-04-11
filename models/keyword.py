import datetime

from cached_property import cached_property
from sqlalchemy import orm

from app import db
from app import get_apiurl_from_openalex_url
from app import logger
from app import KEYWORDS_INDEX
from bulk_actions import create_bulk_actions
from models.counts import citation_count_from_elastic, works_count_from_elastic


def as_keyword_openalex_id(id):
    from app import API_HOST

    return f"{API_HOST}/keywords/{id}"


class Keyword(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "keyword"

    keyword_id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
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

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, KEYWORDS_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
            "keyword": self.display_name,
        }
        if return_level == "full":
            response.update(
                {
                    "works_count": works_count_from_elastic(
                        "keywords.id.keyword", self.openalex_id
                    ),
                    "cited_by_count": citation_count_from_elastic(
                        "keywords.id.keyword", self.openalex_id
                    ),
                    "works_api_url": f"https://api.openalex.org/works?filter=keywords.id:{self.openalex_id_short}",
                    "updated_date": datetime.datetime.utcnow().isoformat(),
                    "created_date": self.created_date.isoformat()[0:10]
                    if isinstance(self.created_date, datetime.datetime)
                    else self.created_date[0:10],
                }
            )
        print(response)
        return response

    def __repr__(self):
        return "<Keyword ( {} ) {} {}>".format(
            self.openalex_api_url, self.id, self.display_name
        )


logger.info(f"loading valid keyword IDs")
_valid_keywords = (
    db.session.query(Keyword.keyword_id).options(orm.Load(Keyword).raiseload("*")).all()
)
_valid_keyword_ids = set([k.keyword_id for k in _valid_keywords])


def is_valid_keyword_id(keyword_id):
    return keyword_id and keyword_id in _valid_keyword_ids
