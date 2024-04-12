import datetime

from cached_property import cached_property

from app import db
from app import get_apiurl_from_openalex_url
from app import LICENSES_INDEX
from bulk_actions import create_bulk_actions
from models.counts import citation_count_from_elastic, works_count_from_elastic


def as_license_openalex_id(id):
    from app import API_HOST

    return f"{API_HOST}/licenses/{id}"


class License(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "license"

    license_id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    url = db.Column(db.Text)
    description = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.license_id

    @property
    def openalex_id(self):
        return as_license_openalex_id(self.license_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id

        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, LICENSES_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
        }
        if return_level == "full":
            response.update(
                {
                    "url": self.url,
                    "description": self.description,
                    "works_count": works_count_from_elastic(
                        "locations.license", self.openalex_id
                    ),
                    "cited_by_count": citation_count_from_elastic(
                        "locations.license", self.openalex_id
                    ),
                    "works_api_url": f"https://api.openalex.org/works?filter=locations.license:{self.openalex_id_short}",
                    "updated_date": datetime.datetime.utcnow().isoformat(),
                    "created_date": self.created_date.isoformat()[0:10]
                    if isinstance(self.created_date, datetime.datetime)
                    else self.created_date[0:10],
                }
            )
        return response

    def __repr__(self):
        return "<Keyword ( {} ) {} {}>".format(
            self.openalex_api_url, self.id, self.display_name
        )
