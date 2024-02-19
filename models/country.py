import datetime
from cached_property import cached_property

from sqlalchemy.dialects.postgresql import JSONB

from app import db
from app import COUNTRIES_INDEX
from models.counts import citation_count_from_elastic, works_count_from_api
from bulk_actions import create_bulk_actions


class Country(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "country"

    country_id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    display_name_alternatives = db.Column(JSONB)
    description = db.Column(db.Text)
    wikidata_url = db.Column(db.Text)
    wikipedia_url = db.Column(db.Text)
    continent_id = db.Column(db.Integer, db.ForeignKey("mid.continent.continent_id"))
    is_global_south = db.Column(db.Boolean)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.country_id

    @property
    def openalex_id(self):
        return f"https://openalex.org/countries/{self.id}"

    @property
    def iso_id(self):
        return f"https://www.iso.org/obp/ui/#iso:code:3166:{self.id}"

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, COUNTRIES_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
        }
        if return_level == "full":
            response.update({
                "country_id": self.id,
                "country_code": self.id,
                "description": self.description,
                "display_name_alternatives": self.display_name_alternatives,
                "ids": {
                    "openalex": self.openalex_id,
                    "iso": self.iso_id,
                    "wikidata": self.wikidata_url,
                    "wikipedia": self.wikipedia_url.replace(" ", "_") if self.wikipedia_url else None,
                },
                "continent": self.continent.to_dict(),
                "is_global_south": self.is_global_south,
                "works_count": works_count_from_api("authorships.countries", self.id),
                "cited_by_count": citation_count_from_elastic("authorships.countries", self.id),
                "authors_api_url": f"https://api.openalex.org/authors?filter=last_known_institutions.country_code:{self.id}",
                "institutions_api_url": f"https://api.openalex.org/institutions?filter=country_code:{self.id}",
                "works_api_url": f"https://api.openalex.org/works?filter=authorships.countries:{self.id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return f"<Country {self.id} {self.display_name}>"
