import datetime

from cached_property import cached_property

from app import db
from app import COUNTRIES_INDEX
from models.counts import citation_count_from_elastic, works_count_from_api
from bulk_actions import create_bulk_actions


class Continent(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "continent"

    continent_id = db.Column(db.Integer, primary_key=True)
    display_name = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    def to_dict(self):
        return {
            "id": f"https://wikidata.org/wiki/{self.wikidata_id}",
            "display_name": self.display_name,
        }


class Country(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "country"

    country_id = db.Column(db.Text, primary_key=True)
    display_name = db.Column(db.Text)
    continent_id = db.Column(db.Integer, db.ForeignKey("mid.continent.continent_id"))
    is_global_south = db.Column(db.Boolean)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.country_id

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, COUNTRIES_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            "id": self.id,
            "display_name": self.display_name,
        }
        if return_level == "full":
            response.update({
                "continent": self.continent.to_dict(),
                "is_global_south": self.is_global_south,
                "works_count": works_count_from_api("authorships.countries", self.id),
                "cited_by_count": citation_count_from_elastic("authorships.countries", self.id),
                "authors_api_url": f"https://api.openalex.org/authors?filter=institution.country_code:{self.id}",
                "institutions_api_url": f"https://api.openalex.org/institutions?filter=country_code:{self.id}",
                "works_api_url": f"https://api.openalex.org/works?filter=authorships.countries:{self.id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return f"<Country {self.id} {self.display_name}>"
