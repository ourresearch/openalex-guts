from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
import requests
import os
import urllib.parse
import json
import datetime

from app import db
from app import USER_AGENT
from app import MAX_MAG_ID
from app import get_apiurl_from_openalex_url
from app import get_db_cursor
from util import dictionary_nested_diff
from util import jsonify_fast_no_sort_raw

# alter table institution rename column normalized_name to mag_normalized_name
# alter table institution add column normalized_name varchar(65000)
# update institution set normalized_name=f_normalize_title(institution.mag_normalized_name)

# truncate mid.institution
# insert into mid.institution (select * from legacy.mag_main_affiliations)
# update mid.institution set display_name=replace(display_name, '\t', '') where display_name ~ '\t';



def as_institution_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/I{id}"

def follow_merged_into_id(lookup_institution_id):
    if not lookup_institution_id:
        return lookup_institution_id
    global merged_into_institutions_dict
    merged_id = merged_into_institutions_dict.get(lookup_institution_id, lookup_institution_id)
    return merged_id

class Institution(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    official_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    geonames_city_id = db.Column(db.Text)
    city = db.Column(db.Text)
    region = db.Column(db.Text)
    country = db.Column(db.Text)
    ror_id = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    wiki_page = db.Column(db.Text)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    wikidata_id = db.Column(db.Text)
    wikipedia_json = db.Column(db.Text)
    wikidata_json = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)
    full_updated_date = db.Column(db.DateTime)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)

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
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

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
    def acronyms(self):
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
        response = []
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
        if ror_ids:
            objs = db.session.query(Institution).options(selectinload(Institution.ror).raiseload('*'), orm.Load(Institution).raiseload('*')).filter(Institution.ror_id.in_(ror_ids)).all()
            for obj in objs:
                obj.relationship_status = relationship_dict[obj.ror_id]
            objs = sorted(objs, key=lambda x: (x.relationship_status if x.relationship_status else "", x.display_name if x.display_name else ""))
            response = [obj.to_dict("minimum") for obj in objs]
        return response

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

    # @cached_property
    # def wikidata_id(self):
    #     if not self.wikipedia_data:
    #         return None
    #     data = self.wikipedia_data
    #     try:
    #         page_id = data["query"]["pages"][0]["pageprops"]["wikibase_item"]
    #     except KeyError:
    #         return None
    #     return page_id

    @cached_property
    def wikipedia_data(self):
        if not self.wiki_page:
            return None
        try:
            data = json.loads(self.wikipedia_json)
        except:
            data = None

        if not data:
            wikipedia_page_name = self.wiki_page.rsplit("/", 1)[-1]
            url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageprops%7Cpageimages%7Cpageterms&piprop=original%7Cthumbnail&pilicense=any&titles={wikipedia_page_name}&pithumbsize=100&redirects="
            print(f"calling wikipedia live for {self.openalex_id} with {url}")
            r = requests.get(url, headers={"User-Agent": USER_AGENT})
            # print(r.json())
            data = r.json()
        return data

    # is whatever the wikipedia url redirects to
    @cached_property
    def wikipedia_url_canonical(self):
        if not self.wikipedia_title:
            return None
        encoded = urllib.parse.quote(self.wikipedia_title)
        return f"https://en.wikipedia.org/wiki/{encoded}"

    @cached_property
    def display_name_international(self):
        data = self.wikidata_data
        if not data:
            return {'en': self.display_name}
        try:
            response = data["entities"][self.wikidata_id_short]["labels"]
            response = {d["language"]: d["value"] for d in response.values()}
            return dict(sorted(response.items()))
        except KeyError:
            return {'en': self.display_name}

    @cached_property
    def wikidata_id_short(self):
        if not self.wikidata_id:
            return None
        return self.wikidata_id.replace("https://www.wikidata.org/wiki/", "")

    @cached_property
    def wikidata_data(self):
        if not self.wikidata_id:
            return None
        try:
            data = json.loads(self.wikidata_json)
        except:
            data = None
        if not data:
            url = f"https://www.wikidata.org/wiki/Special:EntityData/{self.wikidata_id_short}.json"
            print(f"calling wikidata live with {url} for {self.openalex_id}")
            r = requests.get(url, headers={"User-Agent": USER_AGENT})
            try:
                data = r.json()
                # are claims too big?
                del data["entities"][self.wikidata_id_short]["claims"]
            except:
                pass
            # print(response)
        return data

    def store(self):
        VERSION_STRING = "new: updated if changed"
        self.insert_dicts = []
        my_dict = self.to_dict()

        if self.stored and (self.stored.merge_into_id == self.merge_into_id):
            if self.stored.json_save:
                # check merged here for everything but concept
                diff = dictionary_nested_diff(json.loads(self.stored.json_save), my_dict, ["updated_date"])
                if not diff:
                    print(f"dictionary not changed, don't save again {self.openalex_id}")
                    return
                print(f"dictionary for {self.openalex_id} new or changed, so save again. Diff: {diff}")

        now = datetime.datetime.utcnow().isoformat()
        self.full_updated_date = now
        my_dict["updated_date"] = now

        json_save = None
        if not self.merge_into_id:
            json_save = jsonify_fast_no_sort_raw(my_dict)
        if json_save and len(json_save) > 65000:
            print("Error: json_save too long for affiliation_id {}, skipping".format(self.openalex_id))
            json_save = None
        self.insert_dicts = [{"JsonInstitutions": {"id": self.affiliation_id,
                                             "updated": now,
                                             "json_save": json_save,
                                             "version": VERSION_STRING,
                                             "merge_into_id": self.merge_into_id
                                             }}]


    @cached_property
    def concepts(self):
        from models.concept import as_concept_openalex_id

        if not self.counts or not self.counts.paper_count:
            return []

        q = """
            select ancestor_id as id, concept.wikidata_id as wikidata, ancestor_name as display_name, ancestor_level as level, round(100 * (0.0+count(distinct affil.paper_id))/counts.paper_count, 1)::float as score
            from mid.institution institution 
            join mid.citation_institutions_mv counts on counts.affiliation_id=institution.affiliation_id            
            join mid.affiliation affil on affil.affiliation_id=institution.affiliation_id            
            join mid.work_concept_for_api_view wc on wc.paper_id=affil.paper_id
            join mid.concept_self_and_ancestors_mv ancestors on ancestors.id=wc.field_of_study
            join mid.concept_for_api_mv concept on concept.field_of_study_id=ancestor_id                        
            where affil.affiliation_id=:institution_id
            group by ancestor_id, concept.wikidata_id, ancestor_name, ancestor_level, counts.paper_count
            order by score desc
            """
        rows = db.session.execute(text(q), {"institution_id": self.institution_id}).fetchall()
        response = [dict(row) for row in rows if row["score"] and row["score"] > 20]
        for row in response:
            row["id"] = as_concept_openalex_id(row["id"])
        return response

    @cached_property
    def display_counts_by_year(self):
        response_dict = {}
        all_rows = self.counts_by_year_papers + self.counts_by_year_citations
        for count_row in all_rows:
            response_dict[count_row.year] = {"year": count_row.year, "works_count": 0, "cited_by_count": 0}
        for count_row in all_rows:
            if count_row.type == "citation_count":
                response_dict[count_row.year]["cited_by_count"] = int(count_row.n)
            else:
                response_dict[count_row.year]["works_count"] = int(count_row.n)

        my_dicts = [counts for counts in response_dict.values() if counts["year"] and counts["year"] >= 2012]
        response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
        return response

    @classmethod
    def matching_institution_name(cls, raw_string):
        from util import normalize_title_like_sql
        if not raw_string:
            return None

        # for backwards compatibility
        remove_stop_words = False
        return normalize_title_like_sql(raw_string, remove_stop_words)


        # sql_for_match = f"""
        #     select f_matching_string(%s) as match_string;
        #     """
        # with get_db_cursor() as cur:
        #     cur.execute(sql_for_match, (raw_string, ))
        #     rows = cur.fetchall()
        #     if rows:
        #         return rows[0]["match_string"]
        # return None

    @classmethod
    def get_institution_ids_from_strings(self, institution_names):
        if not institution_names:
            return []

        api_key = os.getenv("SAGEMAKER_API_KEY")
        data = [{"affiliation_string": inst_name} for inst_name in institution_names]
        headers = {"X-API-Key": api_key}
        api_url = "https://phqjyj20n2.execute-api.us-east-1.amazonaws.com/api/" # institution lookup endpoint

        number_tries = 0

        while True:
            r = requests.post(api_url, json=json.dumps(data), headers=headers)

            if r.status_code == 200:
                try:
                    response_json = r.json()
                    institution_ids = [my_dict["affiliation_id"] for my_dict in response_json]
                    # now replace any that have been merged with what they have been merged into
                    institution_ids = [follow_merged_into_id(inst_id) for inst_id in institution_ids]
                except Exception as e:
                    print(f"Error, not retrying: {e} in get_institution_ids_from_strings with {self.id}, response {r}, called with {api_url} data: {data} headers: {headers}")
                    institution_ids = []
                return institution_ids

            elif r.status_code == 500:
                print(f"Error on try #{number_tries}, now trying again: Error back from API endpoint: {r} {r.status_code}")
                number_tries += 1

            else:
                print(f"Error, not retrying: Error back from API endpoint: {r} {r.status_code} {r.text} for input {data}")
                return []



    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "ror": self.ror_url,
            "display_name": self.display_name,
            "country_code": self.country_code,
            "type": self.ror.ror_type.lower() if (self.ror and self.ror.ror_type) else None,
        }
        # true for embedded related institutions
        if hasattr(self, "relationship_status"):
            response["relationship"] = self.relationship_status

        if return_level == "full":
            response.update({
                "homepage_url": self.official_page,
                "image_url": self.image_url,
                "image_thumbnail_url": self.image_thumbnail_url,
                "display_name_acronyms": self.acronyms,
                "display_name_alternatives": self.aliases,
                "works_count": self.counts.paper_count if self.counts else 0,
                "cited_by_count": self.counts.citation_count if self.counts else 0,
                "ids": {
                    "openalex": self.openalex_id,
                    "ror": self.ror_url,
                    "grid": self.ror.grid_id if self.ror else None,
                    "wikipedia": self.wikipedia_url_canonical,
                    "wikidata": self.wikidata_id,
                    "mag": self.affiliation_id if self.affiliation_id < MAX_MAG_ID else None
                },
                "geo": {
                    "city": self.city,
                    "geonames_city_id": self.geonames_city_id,
                    "region": self.region,
                    "country_code": self.country_code,
                    "country": self.country,
                    "latitude": self.latitude,
                    "longitude": self.longitude,
                },
                "international": {"display_name": self.display_name_international},
                # "labels": self.labels,
                # "links": self.links,
                "associated_institutions": self.relationship_dicts,
                # "ids": self.external_ids,
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts,
                "works_api_url": f"https://api.openalex.org/works?filter=institutions.id:{self.openalex_id_short}",
                # "updated_date": self.full_updated_date.isoformat()[0:10] if isinstance(self.full_updated_date, datetime.datetime) else self.full_updated_date[0:10],
                "updated_date": datetime.datetime.utcnow().isoformat()[0:10],
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })

            # only include non-null IDs
            for id_type in list(response["ids"].keys()):
                if response["ids"][id_type] == None:
                    del response["ids"][id_type]
        return response

    def __repr__(self):
        return "<Institution ( {} ) {} {}>".format(self.openalex_api_url, self.id, self.display_name)


print(f"loading merged_into_institutions_dict")
merged_into_institutions = db.session.query(Institution).options(orm.Load(Institution).raiseload('*')).filter(Institution.merge_into_id != None).all()
merged_into_institutions_dict = dict((inst.affiliation_id, inst.merge_into_id) for inst in merged_into_institutions)
