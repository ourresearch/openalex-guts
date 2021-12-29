from cached_property import cached_property
from sqlalchemy import text
import json

from app import db
from app import MAX_MAG_ID


# truncate mid.author
# insert into mid.author (select * from legacy.mag_main_authors)
# update mid.author set display_name=replace(display_name, '\t', '') where display_name ~ '\t';

def as_author_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/A{id}"

class Author(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author"

    author_id = db.Column(db.BigInteger, primary_key=True)
    display_name = db.Column(db.Text)
    last_known_affiliation_id = db.Column(db.Numeric, db.ForeignKey("mid.institution.affiliation_id"))
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)

    @property
    def last_known_institution_id(self):
        return self.last_known_affiliation_id

    @property
    def openalex_id(self):
        return as_author_openalex_id(self.author_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def last_known_institution_api_url(self):
        if not self.last_known_affiliation_id:
            return None
        return f"http://localhost:5007/institution/id/{self.last_known_affiliation_id}"

    @property
    def orcid_object(self):
        if not self.orcids:
            return None
        return sorted(self.orcids, key=lambda x: x.orcid)[0]

    @property
    def orcid(self):
        if not self.orcid_object:
            return None
        return self.orcid_object.orcid

    @property
    def orcid_url(self):
        if not self.orcid:
            return None
        return "https://orcid.org/{}".format(self.orcid)

    @cached_property
    def all_alternative_names(self):
        response = [name.display_name for name in self.alternative_names]

        # add what we get from orcid
        if self.orcid_data_person:
            try:
                other_name_dicts = self.orcid_data_person["other-names"]["other-name"]
                other_name_dicts = sorted(other_name_dicts, key=lambda x: x["display-index"])
                response += [name["content"] for name in other_name_dicts if name["content"] not in other_name_dicts]
            except TypeError:
                pass
        return response

    @cached_property
    def scopus_url(self):
        if not self.orcid_data_person:
            return None
        for key, value in self.orcid_data_person["external-identifiers"].items():
            if key=="external-identifier" and value:
                for identifier in value:
                    if identifier["external-id-type"] == 'Scopus Author ID':
                        return identifier["external-id-url"]["value"]
        return None

    @cached_property
    def twitter_url(self):
        if not self.orcid_data_person:
            return None
        for key, value in self.orcid_data_person["researcher-urls"].items():
            if key=="researcher-url" and value:
                for identifier in value:
                    if identifier["url-name"] == 'twitter':
                        return identifier["url"]["value"]
        return None


    @cached_property
    def wikipedia_url(self):
        if not self.orcid_data_person:
            return None
        for key, value in self.orcid_data_person["researcher-urls"].items():
            if key=="researcher-url" and value:
                for identifier in value:
                    if identifier["url-name"] == 'Wikipedia Entry':
                        return identifier["url"]["value"]
        return None

    @cached_property
    def orcid_data_person(self):
        if not self.orcid:
            return None
        if not self.orcid_object.orcid_data:
            return None
        my_data = json.loads(self.orcid_object.orcid_data.api_json)
        return my_data.get("person", None)


    def get_insert_dict_fieldnames(self, table_name=None):
        return ["id", "updated", "json_save", "version"]

    def store(self):
        import datetime
        from util import jsonify_fast_no_sort_raw
        VERSION_STRING = "after all primary keys"

        self.json_save = jsonify_fast_no_sort_raw(self.to_dict())

        # has to match order of get_insert_dict_fieldnames
        json_save_escaped = self.json_save.replace("'", "''").replace("%", "%%").replace(":", "\:")
        if len(json_save_escaped) > 65000:
            print("Error: json_save_escaped too long for paper_id {}, skipping".format(self.openalex_id))
            json_save_escaped = None
        self.insert_dicts = [{"mid.json_authors": "({id}, '{updated}', '{json_save}', '{version}')".format(
                                                                  id=self.author_id,
                                                                  updated=datetime.datetime.utcnow().isoformat(),
                                                                  json_save=json_save_escaped,
                                                                  version=VERSION_STRING
                                                                )}]

    @cached_property
    def concepts(self):
        if not self.author_concepts:
            return []
        response = [author_concept.to_dict() for author_concept in self.author_concepts if author_concept.score > 20]
        response = sorted(response, key=lambda x: x["score"], reverse=True)
        return response

    @cached_property
    def display_counts_by_year(self):
        response_dict = {}
        for count_row in self.counts_by_year:
            response_dict[count_row.year] = {"year": count_row.year, "works_count": 0, "cited_by_count": 0}
        for count_row in self.counts_by_year:
            if count_row.type == "citation_count":
                response_dict[count_row.year]["cited_by_count"] = count_row.n
            else:
                response_dict[count_row.year]["works_count"] = count_row.n

        my_dicts = [counts for counts in response_dict.values() if counts["year"] and counts["year"] >= 2012]
        response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
        return response

    def to_dict(self, return_level="full"):
        response = {
                "id": self.openalex_id,
                "orcid": self.orcid_url,
                "display_name": self.display_name,
              }
        if return_level == "full":
            response.update({
                "display_name_alternatives": self.all_alternative_names,
                "works_count": self.paper_count,
                "cited_by_count": self.citation_count,
                "ids": {
                    "openalex": self.openalex_id,
                    "orcid": self.orcid_url,
                    "scopus": self.scopus_url,
                    "twitter": self.twitter_url,
                    "wikipedia": self.wikipedia_url,
                    "mag": self.author_id if self.author_id < MAX_MAG_ID else None
                },
                # "orcid_data_person": self.orcid_data_person,
                "last_known_institution": self.last_known_institution.to_dict("minimum") if self.last_known_institution else None,
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts,
                "works_api_url": f"https://api.openalex.org/works?filter=author.id:{self.openalex_id_short}",
                "updated_date": self.updated_date
            })
        return response

    def __repr__(self):
        return "<Author ( {} ) {}>".format(self.openalex_id, self.display_name)


