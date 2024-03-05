import datetime

from cached_property import cached_property

from app import db
from app import LANGUAGES_INDEX
from models.counts import citation_count_from_elastic, works_count_from_api
from bulk_actions import create_bulk_actions


class Language(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "language"

    language_id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.language_id

    @property
    def openalex_id(self):
        return f"https://openalex.org/languages/{self.id}"

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, LANGUAGES_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
        }
        if return_level == "full":
            response.update({
                "works_count": works_count_from_api("language", self.openalex_id),
                "cited_by_count": citation_count_from_elastic("language", self.id),
                "works_api_url": f"https://api.openalex.org/works?filter=language:{self.id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return f"<Language {self.id} {self.display_name}>"
