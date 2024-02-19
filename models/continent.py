import datetime

from sqlalchemy.dialects.postgresql import JSONB

from app import db, CONTINENTS_INDEX
from bulk_actions import create_bulk_actions


class Continent(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "continent"

    continent_id = db.Column(db.Integer, primary_key=True)
    display_name = db.Column(db.Text)
    display_name_alternatives = db.Column(JSONB)
    description = db.Column(db.Text)
    wikidata_url = db.Column(db.Text)
    wikipedia_url = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @property
    def id(self):
        return self.openalex_id

    @property
    def openalex_id(self):
        return f"https://openalex.org/continents/{self.wikidata_id}"

    def countries_formatted(self):
        countries_sorted = sorted(self.countries, key=lambda x: x.display_name.lower())
        return [country.to_dict("minimum") for country in countries_sorted]

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, CONTINENTS_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
        }
        if return_level == "full":
            response.update({
                "description": self.description,
                "display_name_alternatives": self.display_name_alternatives,
                "ids": {
                    "openalex": self.openalex_id,
                    "wikidata": self.wikidata_url,
                    "wikipedia": self.wikipedia_url.replace(" ", "_") if self.wikipedia_url else None,
                },
                "countries": self.countries_formatted(),
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return f"<Continent {self.id} {self.display_name}>"
