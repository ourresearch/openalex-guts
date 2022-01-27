from cached_property import cached_property
from sqlalchemy import text
import requests
import json
import urllib.parse
import datetime
from sqlalchemy_redshift.dialect import SUPER

from app import db
from app import USER_AGENT
from app import MAX_MAG_ID
from app import get_apiurl_from_openalex_url
from util import jsonify_fast_no_sort_raw


# truncate mid.concept
# insert into mid.concept (select * from legacy.mag_advanced_fields_of_study)

def as_concept_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/C{id}"

class Concept(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept_for_api_mv"

    field_of_study_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    main_type = db.Column(db.Text)
    level = db.Column(db.Numeric)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    wikipedia_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    wikipedia_json = db.Column(db.Text)
    wikidata_json = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.field_of_study_id

    @property
    def openalex_id(self):
        return as_concept_openalex_id(self.field_of_study_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    @cached_property
    def wikidata_id_short(self):
        if not self.wikidata_id:
            return None
        return self.wikidata_id.replace("https://www.wikidata.org/wiki/", "")

    def filter_for_valid_concepts(concept_id_list):
        q = """
        select field_of_study_id
        from mid.concept_deprecated_concepts
        WHERE field_of_study_id in :concept_id_list
        """
        rows = db.session.execute(text(q), {"concept_id_list": concept_id_list}).fetchall()
        invalid_ids = [row[0] for row in rows]
        response = [id for id in concept_id_list if id not in invalid_ids]
        return response

    # @cached_property
    # def ancestors_raw(self):
    #     q = """
    #     WITH RECURSIVE leaf (child_field_of_study_id, child_field, child_level, field_of_study_id, parent_field, parent_level) AS (
    #     SELECT  linking.child_field_of_study_id,
    #             child_fields.display_name as child_field,
    #             child_fields.level as child_level,
    #             linking.field_of_study_id,
    #             parent_fields.display_name as parent_field,
    #             parent_fields.level as parent_level
    #     FROM    legacy.mag_advanced_field_of_study_children linking
    #     JOIN    legacy.mag_advanced_fields_of_study child_fields on child_fields.field_of_study_id=linking.child_field_of_study_id
    #     JOIN    legacy.mag_advanced_fields_of_study parent_fields on parent_fields.field_of_study_id=linking.field_of_study_id
    #     WHERE   child_field_of_study_id = :concept_id
    #     UNION ALL
    #     SELECT  linking2.child_field_of_study_id,
    #             child_fields2.display_name as child_field,
    #             child_fields2.level as child_level,
    #             linking2.field_of_study_id,
    #             parent_fields2.display_name as parent_field,
    #             parent_fields2.level as parent_level
    #     FROM    legacy.mag_advanced_field_of_study_children linking2
    #     JOIN    legacy.mag_advanced_fields_of_study child_fields2 on child_fields2.field_of_study_id=linking2.child_field_of_study_id
    #     JOIN    legacy.mag_advanced_fields_of_study parent_fields2 on parent_fields2.field_of_study_id=linking2.field_of_study_id
    #
    #     INNER JOIN leaf l
    #     On l.field_of_study_id = linking2.child_field_of_study_id
    #     )
    #     SELECT distinct child_field_of_study_id as id, child_field as name, child_level as level, field_of_study_id as ancestor_id, parent_field as ancestor_name, parent_level as ancestor_level FROM leaf
    #
    #     """
    #     rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
    #     return rows
    #
    # @cached_property
    # def ancestors(self):
    #     rows = self.ancestors_raw
    #     row_dict = {row["ancestor_id"]: row for row in rows}
    #     ancestors = [{"id": as_concept_openalex_id(row["ancestor_id"]),
    #                 "display_name": row["ancestor_name"],
    #                 "level": row["ancestor_level"]} for row in row_dict.values()]
    #     ancestors = sorted(ancestors, key=lambda x: (x["level"], x["display_name"]), reverse=True)
    #     return ancestors

    @cached_property
    def extended_attributes(self):
        q = """
        select attribute_type, attribute_value
        from legacy.mag_advanced_field_of_study_extended_attributes
        WHERE field_of_study_id = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        extended_attributes = [{"attribute_type": row["attribute_type"],
                    "attribute_value": row["attribute_value"]} for row in rows]
        return extended_attributes

    @cached_property
    def umls_aui_urls(self):
        return [attr["attribute_value"] for attr in self.extended_attributes if attr["attribute_type"]==1]

    @cached_property
    def raw_wikipedia_url(self):
        # for attr in self.extended_attributes:
        #     if attr["attribute_type"]==2:
        #         return attr["attribute_value"]

        # temporary
        # page_title = urllib.parse.quote(self.display_name)
        page_title = urllib.parse.quote(self.display_name.lower().replace(" ", "_"))
        return f"https://en.wikipedia.org/wiki/{page_title}"

    @cached_property
    def umls_cui_urls(self):
        return [attr["attribute_value"] for attr in self.extended_attributes if attr["attribute_type"]==3]

    @cached_property
    def wikipedia_data_url(self):
        # for attr_dict in self.extended_attributes:
        #     if attr_dict["attribute_type"] == 2:
        #         wiki_url = attr_dict["attribute_value"]
        #         page_title = wiki_url.rsplit("/", 1)[-1]
        #         url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
        #         return url

        # temporary
        # page_title = urllib.parse.quote(self.display_name)
        page_title = urllib.parse.quote(self.display_name.lower().replace(" ", "_"))
        url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
        return url

    @cached_property
    def related_concepts(self):
        q = """
        select type2, field_of_study_id2, 
        concept2.display_name as field_of_study_id2_display_name,
        concept2.level as field_of_study_id2_level,
        related.rank        
        from legacy.mag_advanced_related_field_of_study related
        join mid.concept_for_api_mv concept2 on concept2.field_of_study_id = field_of_study_id2
        WHERE field_of_study_id1 = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        # not including type on purpose
        related_concepts1 = [{"id": as_concept_openalex_id(row["field_of_study_id2"]),
                              "wikidata": None,
                            "display_name": row["field_of_study_id2_display_name"],
                            "level": row["field_of_study_id2_level"],
                            "score": row["rank"]
                            } for row in rows]

        q = """
        select type1, field_of_study_id1, 
        concept1.display_name as field_of_study_id1_display_name,
        concept1.level as field_of_study_id1_level,        
        related.rank       
        from legacy.mag_advanced_related_field_of_study related
        join mid.concept_for_api_mv concept1 on concept1.field_of_study_id = field_of_study_id1
        WHERE field_of_study_id2 = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        # not including type on purpose
        related_concepts2 = [{"id": as_concept_openalex_id(row["field_of_study_id1"]),
                              "wikidata": None,
                            "display_name": row["field_of_study_id1_display_name"],
                            "level": row["field_of_study_id1_level"],
                            "score": row["rank"]
                            } for row in rows]

        related_concepts_all = related_concepts1 + related_concepts2

        related_concepts_dict = {}
        for row in related_concepts_all:
            related_concepts_dict[row["id"]] = row
        #do it this way to dedup
        related_concepts_all = sorted(related_concepts_dict.values(), key=lambda x: (x["score"]), reverse=True)
        # the ones with poor rank aren't good enough to include
        related_concepts_all = [field for field in related_concepts_all if field["score"] >= 0.75 and field["level"] <= self.level + 1]
        return related_concepts_all

    @cached_property
    def image_url(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["original"]["source"]
        except KeyError:
            return None

        return page_id

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
    def description(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["terms"]["description"][0]
        except KeyError:
            return None

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
    def raw_wikidata_id(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["pageprops"]["wikibase_item"]
        except KeyError:
            return None
        return page_id

    @cached_property
    def wikipedia_url(self):
        return self.wikipedia_id

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
            data = r.json()
            # are claims too big?
            try:
                del data["entities"][self.wikidata_id_short]["claims"]
            except:
                pass
            # print(response)
        return data


    @cached_property
    def wikipedia_data(self):
        try:
            return json.loads(self.wikipedia_json)
        except:
            print(f"Error doing json_loads for {self.openalex_id} in wikipedia_data")
            return None

    @cached_property
    def raw_wikipedia_data(self):
        if not self.wikipedia_url:
            return None
        wikipedia_page_name = self.wikipedia_url.rsplit("/", 1)[-1]

        # print(f"\noriginal: {self.wikipedia_url} for name {self.display_name}")
        url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageprops%7Cpageimages%7Cpageterms&piprop=original%7Cthumbnail&titles={wikipedia_page_name}&pithumbsize=100&redirects="
        # print(f"calling {url}")
        r = requests.get(url, headers={"User-Agent": USER_AGENT})
        # print(r.json())

        return r.json()


    @cached_property
    def display_name_international(self):
        if not self.wikidata_data:
            return None
        data = self.wikidata_data
        try:
            response = data["entities"][self.wikidata_id_short]["labels"]
            response = {d["language"]: d["value"] for d in response.values()}
            return dict(sorted(response.items()))
        except KeyError:
            return None

    @cached_property
    def description(self):
        if not self.description_international:
            return None
        try:
            return self.description_international["en"]
        except KeyError:
            return None

    @cached_property
    def description_international(self):
        if not self.wikidata_data:
            return None
        data = self.wikidata_data
        try:
            response = data["entities"][self.wikidata_id_short]["descriptions"]
            response = {d["language"]: d["value"] for d in response.values()}
            return dict(sorted(response.items()))
        except KeyError:
            return None

    @cached_property
    def raw_wikidata_data(self):
        if not self.wikidata_id_short:
            return None

        url = f"https://www.wikidata.org/wiki/Special:EntityData/{self.wikidata_id_short}.json"
        print(f"calling {url}")
        r = requests.get(url, headers={"User-Agent": USER_AGENT})
        response = r.json()
        # claims are too big
        try:
            del response["entities"][self.wikidata_id_short]["claims"]
        except KeyError:
            # not here for some reason, doesn't matter
            pass
        response_json = json.dumps(response, ensure_ascii=False)
        # work around redshift bug with nested quotes in json
        response = response_json.replace('\\"', '*')
        return response

    def store(self):
        VERSION_STRING = "after all primary keys"

        self.json_save = jsonify_fast_no_sort_raw(self.to_dict())

        # has to match order of get_insert_dict_fieldnames
        if len(self.json_save) > 65000:
            print("Error: json_save_escaped too long for paper_id {}, skipping".format(self.openalex_id))
            self.json_save = None
        updated = datetime.datetime.utcnow().isoformat()
        self.insert_dicts = [{"JsonConcepts": [self.field_of_study_id, updated, self.json_save, VERSION_STRING]}]

    def save_wiki(self):
        if not hasattr(self, "insert_dicts"):
            # wikipedia_data = json.dumps(self.raw_wikipedia_data, ensure_ascii=False).replace("'", "''").replace("%", "%%").replace(":", "\:")
            # if len(wikipedia_data) > 64000:
            #     wikipedia_data = None
            wikidata_data = json.dumps(self.raw_wikidata_data, ensure_ascii=False).replace("'", "''").replace("%", "%%").replace(":", "\:")
            if len(wikidata_data) > 64000:
                wikidata_data = None
            self.insert_dicts = [{"ins.wiki_concept": "({id}, '{wikidata_super}')".format(
                                  id=self.field_of_study_id,
                                  wikidata_super=wikidata_data,
                                )}]

    def clean_metadata(self):
        if not self.metadata:
            return

        self.metadata.updated = datetime.datetime.utcnow()
        self.wikipedia_id = self.metadata.wikipedia_id
        self.wikidata_id_short = self.metadata.wikidata_id_short

        return

        # work around redshift bug with nested quotes in json
        if self.metadata.wikipedia_json:
            response = json.loads(self.metadata.wikipedia_json.replace('\\\\"', '*'))
            # response = self.metadata.wikipedia_json.replace('\\\\"', '*')
            self.wikipedia_super = response

        # try:
        #     # work around redshift bug with nested quotes in json
        #     response = json.loads(self.metadata.wikipedia_json.replace('\\\\"', '*'))
        #     self.wikipedia_super = json.loads(response)
        # except:
        #     print(f"Error: oops on loading wikipedia_super {self.field_of_study_id}")
        #     pass

        if self.metadata.wikidata_json:
            # self.wikidata_super = json.loads(self.metadata.wikidata_json.replace('\\\\"', '*'))
            self.wikidata_super = json.loads(self.metadata.wikidata_json.replace('\\\\"', '*'))
        elif self.metadata.wikidata_id_short:
            print("getting wikidata")
            self.wikidata_super = self.raw_wikidata_data

        # try:
        #     if self.metadata.wikidata_json:
        #         self.wikidata_super = json.loads(self.metadata.wikidata_json)
        #     elif self.metadata.wikidata_id_short:
        #         print("getting wikidata")
        #         self.wikidata_super = self.raw_wikidata_data
        # except:
        #     print(f"Error: oops on loading wikidata_super {self.field_of_study_id}")
        #     pass

        

    def store_ancestors(self):
        ancestors = self.ancestors_raw
        if not hasattr(self, "insert_dicts"):
            self.insert_dicts = []
        for ancestor in ancestors:
            self.insert_dicts += [{"mid.concept_ancestor": "({id}, '{name}', {level}, {ancestor_id}, '{ancestor_name}', {ancestor_level})".format(
                                  id=self.field_of_study_id,
                                  name=self.display_name.replace("'", ""),
                                  level=self.level,
                                  ancestor_id=ancestor["ancestor_id"],
                                  ancestor_name=ancestor["ancestor_name"].replace("'", ""),
                                  ancestor_level=ancestor["ancestor_level"],
                                )}]


    @cached_property
    def ancestors_sorted(self):
        if not self.ancestors:
            return []
        non_null_ancestors = [ancestor for ancestor in self.ancestors if ancestor and ancestor.my_ancestor]
        return sorted(non_null_ancestors, key=lambda x: (-1 * x.my_ancestor.level, x.my_ancestor.display_name), reverse=False)

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
            "wikidata": self.wikidata_id,
            "display_name": self.display_name,
            "level": self.level,
        }
        if return_level == "full":
            response.update({
                "description": self.description,
                "works_count": self.paper_count if self.paper_count else 0,
                "cited_by_count": self.citation_count,
                "ids": {
                    "openalex": self.openalex_id,
                    "wikidata": self.wikidata_id,
                    "wikipedia": self.wikipedia_url,
                    "umls_aui": self.umls_aui_urls,
                    "umls_cui": self.umls_cui_urls,
                    "mag": self.field_of_study_id if self.field_of_study_id < MAX_MAG_ID else None
                },
                "image_url": self.image_url,
                "image_thumbnail_url": self.image_thumbnail_url,
                "international": {
                    "display_name": self.display_name_international,
                    "description": self.description_international
                },
                "ancestors": [ancestor.my_ancestor.to_dict("minimal") for ancestor in self.ancestors_sorted],
                "related_concepts": self.related_concepts,
                "counts_by_year": self.display_counts_by_year,
                "works_api_url": f"https://api.openalex.org/works?filter=concepts.id:{self.openalex_id_short}",
                "updated_date": self.updated_date
            })

            # only include non-null IDs
            for id_type in list(response["ids"].keys()):
                if response["ids"][id_type] == None:
                    del response["ids"][id_type]
        return response

    def __repr__(self):
        return "<Concept ( {} ) {}>".format(self.openalex_api_url, self.display_name)


