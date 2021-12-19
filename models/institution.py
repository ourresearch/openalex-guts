from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
import requests
import urllib.parse
import json

from app import db
from app import USER_AGENT

# alter table institution rename column normalized_name to mag_normalized_name
# alter table institution add column normalized_name varchar(65000)
# update institution set normalized_name=f_normalize_title(institution.mag_normalized_name)

# truncate mid.institution
# insert into mid.institution (select * from legacy.mag_main_affiliations)
# update mid.institution set display_name=replace(display_name, '\t', '') where display_name ~ '\t';

def as_institution_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/I{id}"

class Institution(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    official_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    ror_id = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    wiki_page = db.Column(db.Text)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.affiliation_id

    @property
    def openalex_id(self):
        return as_institution_openalex_id(self.affiliation_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def institution_id(self):
        return self.affiliation_id

    @property
    def institution_display_name(self):
        if self.ror:
            return self.ror.name
        return self.display_name

    @property
    def ror_url(self):
        if self.ror_id:
            return "https://ror.org/{}".format(self.ror_id)
        return None

    @property
    def country_code(self):
        if not self.iso3166_code:
            return None
        return self.iso3166_code.upper()

    # @cached_property
    # def wikipedia_data_url(self):
    #     if self.wiki_page:
    #         page_title = self.wiki_page.rsplit("/", 1)[-1]
    #         url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
    #         return url
    #     return None

    @cached_property
    def acroynyms(self):
        q = """
        select acronym
        from ins.ror_acronyms
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def aliases(self):
        q = """
        select alias
        from ins.ror_aliases
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def external_ids(self):
        q = """
        select external_id_type, external_id
        from ins.ror_external_ids
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [{"type": row[0], "id": row[1]} for row in rows]
        return response

    @cached_property
    def geonames_city_id(self):
        # q = """
        # select geonames_city_id
        # from ins.ror_geonames
        # join ins.ror_addresses on ror_addresses.geonames_city_id = ror_geonames.geonames_city_id
        # WHERE ror_id = :ror_id
        # """
        q = """
        select geonames_city_id
        from ins.ror_addresses
        WHERE ror_id = :ror_id
        """
        row = db.session.execute(text(q), {"ror_id": self.ror_id}).first()
        if not row:
            return None
        return row[0]

    @cached_property
    def labels(self):
        q = """
        select *
        from ins.ror_labels
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [dict(row) for row in rows]
        return response

    @cached_property
    def links(self):
        q = """
        select link
        from ins.ror_links
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def relationship_dicts(self):
        q = """
           select relationship_type, ror_grid_equivalents.ror_id as related_ror_id 
            from ins.ror_relationships
            join ins.ror_grid_equivalents on ror_grid_equivalents.grid_id = ror_relationships.related_grid_id
            WHERE ror_relationships.ror_id = :ror_id        
            """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        relationship_dict = {}
        for row in rows:
            relationship_dict[row["related_ror_id"]] = row["relationship_type"].lower()
        ror_ids = relationship_dict.keys()
        objs = db.session.query(Institution).options(selectinload(Institution.ror), orm.Load(Institution).raiseload('*')).filter(Institution.ror_id.in_(ror_ids)).all()
        for obj in objs:
            obj.relationship_status = relationship_dict[obj.ror_id]
        objs = sorted(objs, key=lambda x: (x.relationship_status if x.relationship_status else "", x.display_name if x.display_name else ""))
        response = [obj.to_dict("minimum") for obj in objs]
        return response

    @cached_property
    def type(self):
        q = """
        select type
        from ins.ror_types
        WHERE ror_id = :ror_id
        """
        row = db.session.execute(text(q), {"ror_id": self.ror_id}).first()
        return row[0].lower() if row else None

    @cached_property
    def wikipedia_pageid(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["pageid"]
        except KeyError:
            return None

        return page_id

    @cached_property
    def image_url(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        image_url = None
        try:
            image_url = data["query"]["pages"][0]["original"]["source"]
        except KeyError:
            pass
        return image_url

    @cached_property
    def image_thumbnail_url(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["thumbnail"]["source"]
        except KeyError:
            return None

        return page_id

    @cached_property
    def wikipedia_title(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        # print(data)
        try:
            return data["query"]["pages"][0]["title"]
        except KeyError:
            return None

    @cached_property
    def wikidata_url(self):
        if not self.wikidata_id:
            return None
        return f"https://www.wikidata.org/wiki/{self.wikidata_id}"

    @cached_property
    def wikidata_id(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["pageprops"]["wikibase_item"]
        except KeyError:
            return None
        return page_id

    @cached_property
    def wikipedia_data(self):
        if not self.wiki_page:
            return None
        wikipedia_page_name = self.wiki_page.rsplit("/", 1)[-1]
        url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageprops%7Cpageimages%7Cpageterms&piprop=original%7Cthumbnail&pilicense=any&titles={wikipedia_page_name}&pithumbsize=100&redirects="
        # print(url)
        r = requests.get(url, headers={"User-Agent": USER_AGENT})
        # print(r.json())
        return r.json()

    # is whatever the wikipedia url redirects to
    @cached_property
    def wikipedia_url_canonical(self):
        if not self.wikipedia_title:
            return None
        encoded = urllib.parse.quote(self.wikipedia_title)
        return f"https://en.wikipedia.org/wiki/{encoded}"

    @cached_property
    def display_name_international(self):
        if not self.wikidata_data:
            return None
        data = self.wikidata_data
        try:
            response = data["entities"][self.wikidata_id]["labels"]
            response = {d["language"]: d["value"] for d in response.values()}
            return dict(sorted(response.items()))
        except KeyError:
            return None

    @cached_property
    def wikidata_data(self):
        if not self.wikidata_id:
            return None
        url = f"https://www.wikidata.org/wiki/Special:EntityData/{self.wikidata_id}.json"
        r = requests.get(url, headers={"User-Agent": USER_AGENT})
        # print(r.json())
        r = requests.get(url, headers={"User-Agent": USER_AGENT})
        response = r.json()
        # are claims too big?
        # del response["entities"][self.wikidata_id]["claims"]
        return response

    def get_insert_dict_fieldnames(self, table_name=None):
        lookup = {
            "ins.wiki_institution": ["affiliation_id", "ror_id", "wikipedia_id", "wikidata_id", "wikipedia_json", "wikidata_json"],
            "mid.json_institutions": ["id", "updated", "json_save", "version"]
        }
        if table_name:
            return lookup[table_name]
        return lookup


    def store(self):
        import datetime
        from util import jsonify_fast_no_sort_raw
        VERSION_STRING = "sent to casey"

        self.json_save = jsonify_fast_no_sort_raw(self.to_dict())

        # has to match order of get_insert_dict_fieldnames
        json_save_escaped = self.json_save.replace("'", "''").replace("%", "%%").replace(":", "\:")
        if len(json_save_escaped) > 65000:
            print("Error: json_save_escaped too long for paper_id {}, skipping".format(self.openalex_id))
            json_save_escaped = None
        self.insert_dicts = [{"mid.json_institutions": "({id}, '{updated}', '{json_save}', '{version}')".format(
                                                                  id=self.affiliation_id,
                                                                  updated=datetime.datetime.utcnow().isoformat(),
                                                                  json_save=json_save_escaped,
                                                                  version=VERSION_STRING
                                                                )}]

    def save_wiki(self):
        if not hasattr(self, "insert_dicts"):
            wikipedia_data = json.dumps(self.wikipedia_data).replace("'", "''").replace("%", "%%").replace(":", "\:")
            if len(wikipedia_data) > 64000:
                wikipedia_data = None
            wikidata_data = json.dumps(self.wikidata_data).replace("'", "''").replace("%", "%%").replace(":", "\:")
            if len(wikidata_data) > 64000:
                wikidata_data = None
            self.insert_dicts = [{"ins.wiki_institution": "({id}, '{ror_id}', '{wikipedia_id}', '{wikidata_id}', '{wikipedia_data}', '{wikidata_data}')".format(
                                  id=self.affiliation_id,
                                  ror_id=self.ror_id,
                                  wikipedia_id=self.wikipedia_url_canonical,
                                  wikidata_id=self.wikidata_url,
                                  wikipedia_data=wikipedia_data,
                                  wikidata_data=wikidata_data
                                )}]

    @cached_property
    def concepts(self):
        from models.concept import as_concept_openalex_id

        q = """
            select ancestor_id as id, ancestor_name as display_name, ancestor_level as level, round(100 * count(distinct affil.paper_id)/institution.paper_count::float, 1) as score
            from mid.institution institution 
            join mid.affiliation affil on affil.affiliation_id=institution.affiliation_id            
            join mid.work_concept wc on wc.paper_id=affil.paper_id
            join mid.concept_self_and_ancestors_view ancestors on ancestors.id=wc.field_of_study
            where affil.affiliation_id=:institution_id
            group by ancestor_id, ancestor_name, ancestor_level, institution.paper_count
            order by score desc
            """
        rows = db.session.execute(text(q), {"institution_id": self.institution_id}).fetchall()
        response = [dict(row) for row in rows if row["score"] > 20]
        for row in response:
            row["id"] = as_concept_openalex_id(row["id"])
        return response

    @cached_property
    def display_counts_by_year(self):
        response_dict = {}
        for count_row in self.counts_by_year:
            response_dict[count_row.year] = {"year": count_row.year, "works_count": 0, "cited_by_count": 0}
            if count_row.type == "citation_count":
                response_dict[count_row.year]["cited_by_count"] = count_row.n
            else:
                response_dict[count_row.year]["works_count"] = count_row.n

        my_dicts = [counts for counts in response_dict.values() if counts["year"] >= 2012]
        response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
        return response

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "ror": self.ror_url,
            "display_name": self.display_name,
            "country_code": self.ror.country_code_upper if self.ror else self.country_code,
            "type": self.type,
        }
        # true for embedded related institutions
        if hasattr(self, "relationship_status"):
            response["relationship"] = self.relationship_status

        if return_level == "full":
            response.update({
                "homepage_url": self.official_page,
                "image_url": self.image_url,
                "image_thumbnail_url": self.image_thumbnail_url,
                "display_name_acroynyms": self.acroynyms,
                "display_name_alternatives": self.aliases,
                "works_count": self.paper_count,
                "cited_by_count": self.citation_count,
                "ids": {
                    "openalex": self.openalex_id,
                    "ror": self.ror_url,
                    "grid": self.ror.grid_id if self.ror else None,
                    "wikipedia": self.wikipedia_url_canonical,
                    "wikidata": self.wikidata_url
                },
                "geo": {
                    "city": self.ror.city if self.ror else None,
                    "geonames_city_id": self.geonames_city_id,
                    "region": self.ror.state if self.ror else None,
                    "country_code": self.ror.country_code_upper if self.ror else self.country_code,
                    "country": self.ror.country if self.ror else None,
                    "latitude": self.latitude,
                    "longitude": self.longitude,
                },
                "international": {"display_name": self.display_name_international},
                # "labels": self.labels,
                # "links": self.links,
                "associated_insitutions": self.relationship_dicts,
                # "ids": self.external_ids,
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts,
                "works_api_url": f"https://api.openalex.org/works?filter=institution.id:{self.openalex_id_short}",
                "updated_date": self.updated_date,
            })

        return response

    def __repr__(self):
        return "<Institution ( {} ) {}>".format(self.openalex_id, self.display_name)


