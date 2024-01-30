from dataclasses import field
import datetime
import json
import urllib.parse

import requests
from cached_property import cached_property
from sqlalchemy import orm
from sqlalchemy import text

from app import MAX_MAG_ID
from app import USER_AGENT
from app import db
from app import get_apiurl_from_openalex_url
from app import logger
from util import entity_md5


# truncate mid.concept
# insert into mid.concept (select * from legacy.mag_advanced_fields_of_study)

def as_topic_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/T{id}"


class Topic(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "topic"

    topic_id = db.Column(db.Integer, primary_key=True)
    display_name = db.Column(db.Text)
    summary = db.Column(db.Text)
    keywords = db.Column(db.Text)
    subfield_id = db.Column(db.Integer)
    field_id = db.Column(db.Integer)
    domain_id = db.Column(db.Integer)
    wikipedia_url = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @cached_property
    def id(self):
        return self.topic_id

    @property
    def openalex_id(self):
        return as_topic_openalex_id(self.topic_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)


    # @cached_property
    # def extended_attributes(self):
    #     q = """
    #     select attribute_type, attribute_value
    #     from legacy.mag_advanced_field_of_study_extended_attributes
    #     WHERE field_of_study_id = :concept_id
    #     """
    #     rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
    #     extended_attributes = [{"attribute_type": row["attribute_type"],
    #                 "attribute_value": row["attribute_value"]} for row in rows]
    #     return extended_attributes

    # @cached_property
    # def umls_aui_urls(self):
    #     return [attr["attribute_value"] for attr in self.extended_attributes if attr["attribute_type"]==1]

    # @cached_property
    # def raw_wikipedia_url(self):
    #     # for attr in self.extended_attributes:
    #     #     if attr["attribute_type"]==2:
    #     #         return attr["attribute_value"]

    #     # temporary
    #     # page_title = urllib.parse.quote(self.display_name)
    #     page_title = urllib.parse.quote(self.display_name.lower().replace(" ", "_"))
    #     return f"https://en.wikipedia.org/wiki/{page_title}"

    # @cached_property
    # def umls_cui_urls(self):
    #     return [attr["attribute_value"] for attr in self.extended_attributes if attr["attribute_type"]==3]

    # @cached_property
    # def wikipedia_data_url(self):
    #     # for attr_dict in self.extended_attributes:
    #     #     if attr_dict["attribute_type"] == 2:
    #     #         wiki_url = attr_dict["attribute_value"]
    #     #         page_title = wiki_url.rsplit("/", 1)[-1]
    #     #         url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
    #     #         return url

    #     # temporary
    #     # page_title = urllib.parse.quote(self.display_name)
    #     page_title = urllib.parse.quote(self.display_name.lower().replace(" ", "_"))
    #     url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
    #     return url

    # @cached_property
    # def related_concepts(self):
    #     q = """
    #     select type2, field_of_study_id2, 
    #     concept2.display_name as field_of_study_id2_display_name,
    #     concept2.level as field_of_study_id2_level,
    #     related.rank        
    #     from legacy.mag_advanced_related_field_of_study related
    #     join mid.concept_for_api_mv concept2 on concept2.field_of_study_id = field_of_study_id2
    #     WHERE field_of_study_id1 = :concept_id
    #     """
    #     rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
    #     # not including type on purpose
    #     related_concepts1 = [{"id": as_concept_openalex_id(row["field_of_study_id2"]),
    #                           "wikidata": None,
    #                         "display_name": row["field_of_study_id2_display_name"],
    #                         "level": row["field_of_study_id2_level"],
    #                         "score": row["rank"]
    #                         } for row in rows]

    #     q = """
    #     select type1, field_of_study_id1, 
    #     concept1.display_name as field_of_study_id1_display_name,
    #     concept1.level as field_of_study_id1_level,        
    #     related.rank       
    #     from legacy.mag_advanced_related_field_of_study related
    #     join mid.concept_for_api_mv concept1 on concept1.field_of_study_id = field_of_study_id1
    #     WHERE field_of_study_id2 = :concept_id
    #     """
    #     rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
    #     # not including type on purpose
    #     related_concepts2 = [{"id": as_concept_openalex_id(row["field_of_study_id1"]),
    #                           "wikidata": None,
    #                         "display_name": row["field_of_study_id1_display_name"],
    #                         "level": row["field_of_study_id1_level"],
    #                         "score": row["rank"]
    #                         } for row in rows]

    #     related_concepts_all = related_concepts1 + related_concepts2

    #     related_concepts_dict = {}
    #     for row in related_concepts_all:
    #         related_concepts_dict[row["id"]] = row
    #     #do it this way to dedup
    #     related_concepts_all = sorted(related_concepts_dict.values(), key=lambda x: (x["score"]), reverse=True)
    #     # the ones with poor rank aren't good enough to include
    #     related_concepts_all = [field for field in related_concepts_all if field["score"] >= 0.75 and field["level"] <= self.level + 1]
    #     # keep a max of 100 related concepts
    #     related_concepts_all = related_concepts_all[:100]
    #     return related_concepts_all

    # @cached_property
    # def image_url(self):
    #     if not self.wikipedia_data:
    #         return None
    #     data = self.wikipedia_data
    #     try:
    #         page_id = data["query"]["pages"][0]["original"]["source"]
    #     except KeyError:
    #         return None

    #     return page_id

    # @cached_property
    # def image_thumbnail_url(self):
    #     if not self.wikipedia_data:
    #         return None
    #     data = self.wikipedia_data
    #     try:
    #         page_id = data["query"]["pages"][0]["thumbnail"]["source"]
    #     except KeyError:
    #         return None

    #     return page_id

    # @cached_property
    # def description(self):
    #     if not self.wikipedia_data:
    #         return None
    #     data = self.wikipedia_data
    #     try:
    #         page_id = data["query"]["pages"][0]["terms"]["description"][0]
    #     except KeyError:
    #         return None

    # @cached_property
    # def wikipedia_title(self):
    #     if not self.wikipedia_data:
    #         return None
    #     data = self.wikipedia_data
    #     # print(data)
    #     try:
    #         return data["query"]["pages"][0]["title"]
    #     except KeyError:
    #         return None

    # @cached_property
    # def raw_wikidata_id(self):
    #     if not self.wikipedia_data:
    #         return None
    #     data = self.wikipedia_data
    #     try:
    #         page_id = data["query"]["pages"][0]["pageprops"]["wikibase_item"]
    #     except KeyError:
    #         return None
    #     return page_id

    # @cached_property
    # def wikipedia_url(self):
    #     return self.wikipedia_id

    # @cached_property
    # def wikidata_data(self):
    #     if not self.wikidata_id:
    #         return None
    #     try:
    #         data = json.loads(self.wikidata_json)
    #     except:
    #         data = None
    #     if not data:
    #         url = f"https://www.wikidata.org/wiki/Special:EntityData/{self.wikidata_id_short}.json"
    #         logger.info(f"calling wikidata live with {url} for {self.openalex_id}")
    #         r = requests.get(url, headers={"User-Agent": USER_AGENT})
    #         data = r.json()
    #         # are claims too big?
    #         try:
    #             del data["entities"][self.wikidata_id_short]["claims"]
    #         except:
    #             pass
    #     return data


    # @cached_property
    # def wikipedia_data(self):
    #     try:
    #         return json.loads(self.wikipedia_json)
    #     except:
    #         logger.exception(f"Error doing json_loads for {self.openalex_id} in wikipedia_data")
    #         return None

    # @cached_property
    # def raw_wikipedia_data(self):
    #     if not self.wikipedia_url:
    #         return None
    #     wikipedia_page_name = self.wikipedia_url.rsplit("/", 1)[-1]

    #     url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageprops%7Cpageimages%7Cpageterms&piprop=original%7Cthumbnail&titles={wikipedia_page_name}&pithumbsize=100&redirects="
    #     r = requests.get(url, headers={"User-Agent": USER_AGENT})

    #     return r.json()


    # @cached_property
    # def display_name_international(self):
    #     if not self.wikidata_data:
    #         return None
    #     data = self.wikidata_data
    #     try:
    #         response = data["entities"][self.wikidata_id_short]["labels"]
    #         response = {d["language"]: d["value"] for d in response.values()}
    #         return dict(sorted(response.items()))
    #     except KeyError:
    #         return None

    # @cached_property
    # def description(self):
    #     if not self.description_international:
    #         return None
    #     try:
    #         return self.description_international["en"]
    #     except KeyError:
    #         return None

    # @cached_property
    # def description_international(self):
    #     if not self.wikidata_data:
    #         return None
    #     data = self.wikidata_data
    #     try:
    #         response = data["entities"][self.wikidata_id_short]["descriptions"]
    #         response = {d["language"]: d["value"] for d in response.values()}
    #         return dict(sorted(response.items()))
    #     except KeyError:
    #         return None

    # @cached_property
    # def raw_wikidata_data(self):
    #     if not self.wikidata_id_short:
    #         return None

    #     url = f"https://www.wikidata.org/wiki/Special:EntityData/{self.wikidata_id_short}.json"
    #     logger.info(f"calling {url}")
    #     r = requests.get(url, headers={"User-Agent": USER_AGENT})
    #     response = r.json()
    #     # claims are too big
    #     try:
    #         del response["entities"][self.wikidata_id_short]["claims"]
    #     except KeyError:
    #         # not here for some reason, doesn't matter
    #         pass
    #     response_json = json.dumps(response, ensure_ascii=False)
    #     # work around redshift bug with nested quotes in json
    #     response = response_json.replace('\\"', '*')
    #     return response

    # def store(self):
    #     bulk_actions = []

    #     my_dict = self.to_dict()
    #     my_dict['updated'] = my_dict.get('updated_date')
    #     my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
    #     my_dict['@version'] = 1
    #     entity_hash = entity_md5(my_dict)
    #     old_entity_hash = self.json_entity_hash and self.json_entity_hash.json_entity_hash

    #     if entity_hash != old_entity_hash:
    #         logger.info(f"dictionary for {self.openalex_id} new or changed, so save again")
    #         index_record = {
    #             "_op_type": "index",
    #             "_index": "topics-v1",
    #             "_id": self.openalex_id,
    #             "_source": my_dict
    #         }
    #         bulk_actions.append(index_record)
    #     else:
    #         logger.info(f"dictionary not changed, don't save again {self.openalex_id}")

    #     db.session.merge(
    #         TopicJsonEntityHash(
    #             topic_id=self.topic_id,
    #             json_entity_hash=entity_hash
    #         )
    #     )
    #     return bulk_actions

    # def clean_metadata(self):
    #     if not self.metadata:
    #         return

    #     self.metadata.updated = datetime.datetime.utcnow()
    #     self.wikipedia_id = self.metadata.wikipedia_id
    #     self.wikidata_id_short = self.metadata.wikidata_id_short

    #     return

    #     # work around redshift bug with nested quotes in json
    #     if self.metadata.wikipedia_json:
    #         response = json.loads(self.metadata.wikipedia_json.replace('\\\\"', '*'))
    #         # response = self.metadata.wikipedia_json.replace('\\\\"', '*')
    #         self.wikipedia_super = response

    #     # try:
    #     #     # work around redshift bug with nested quotes in json
    #     #     response = json.loads(self.metadata.wikipedia_json.replace('\\\\"', '*'))
    #     #     self.wikipedia_super = json.loads(response)
    #     # except:
    #     #     print(f"Error: oops on loading wikipedia_super {self.field_of_study_id}")
    #     #     pass

    #     if self.metadata.wikidata_json:
    #         # self.wikidata_super = json.loads(self.metadata.wikidata_json.replace('\\\\"', '*'))
    #         self.wikidata_super = json.loads(self.metadata.wikidata_json.replace('\\\\"', '*'))
    #     elif self.metadata.wikidata_id_short:
    #         print("getting wikidata")
    #         self.wikidata_super = self.raw_wikidata_data

    #     # try:
    #     #     if self.metadata.wikidata_json:
    #     #         self.wikidata_super = json.loads(self.metadata.wikidata_json)
    #     #     elif self.metadata.wikidata_id_short:
    #     #         print("getting wikidata")
    #     #         self.wikidata_super = self.raw_wikidata_data
    #     # except:
    #     #     print(f"Error: oops on loading wikidata_super {self.field_of_study_id}")
    #     #     pass


    # def calculate_ancestors(self):
    #     ancestors = self.ancestors_raw
    #     if not hasattr(self, "insert_dicts"):
    #         self.insert_dicts = []
    #     for ancestor in ancestors:
    #         id = self.field_of_study_id
    #         ancestor_id = ancestor["ancestor_id"]
    #         self.insert_dicts += [{"ConceptAncestor": [id, ancestor_id]}]
    #     print(self.insert_dicts)

    # @cached_property
    # def ancestors_sorted(self):
    #     if not self.ancestors:
    #         return []
    #     non_null_ancestors = [ancestor for ancestor in self.ancestors if ancestor and ancestor.my_ancestor]
    #     return sorted(non_null_ancestors, key=lambda x: (-1 * x.my_ancestor.level, x.my_ancestor.display_name), reverse=False)

    # @cached_property
    # def display_counts_by_year(self):
    #     response_dict = {}
    #     all_rows = self.counts_by_year
    #     for count_row in all_rows:
    #         response_dict[count_row.year] = {
    #             "year": count_row.year,
    #             "works_count": 0,
    #             "oa_works_count": 0,
    #             "cited_by_count": 0
    #         }
    #     for count_row in all_rows:
    #         if count_row.type == "citation_count":
    #             response_dict[count_row.year]["cited_by_count"] = int(count_row.n)
    #         elif count_row.type == "oa_paper_count":
    #             response_dict[count_row.year]["oa_works_count"] = int(count_row.n)
    #         else:
    #             response_dict[count_row.year]["works_count"] = int(count_row.n)

    #     my_dicts = [counts for counts in response_dict.values() if counts["year"] and counts["year"] >= 2012]
    #     response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
    #     return response

    # def oa_percent(self):
    #     if not (self.counts and self.counts.paper_count and self.counts.oa_paper_count):
    #         return 0

    #     return min(round(100.0 * float(self.counts.oa_paper_count) / float(self.counts.paper_count), 2), 100)

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
            "subfield": self.subfields.to_dict("minimal"),
            "field": self.fields.to_dict("minimal"),
            "domain": self.domains.to_dict("minimal")
        }
        if return_level == "full":
            response.update({
                "summary": self.summary,
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                # "summary_stats": {
                #     "2yr_mean_citedness": (self.impact_factor and self.impact_factor.impact_factor) or 0,
                #     "h_index": (self.h_index and self.h_index.h_index) or 0,
                #     "i10_index": (self.i10_index and self.i10_index.i10_index) or 0,
                #     "oa_percent": self.oa_percent(),
                #     "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                #     "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                #     "2yr_works_count": int(self.counts_2year.paper_count or 0) if self.counts_2year else 0,
                #     "2yr_cited_by_count": int(self.counts_2year.citation_count or 0) if self.counts_2year else 0,
                #     "2yr_i10_index": int(self.i10_index_2year.i10_index or 0) if self.i10_index_2year else 0,
                #     "2yr_h_index": int(self.h_index_2year.h_index or 0) if self.h_index_2year else 0
                # },
                "ids": {
                    "openalex": self.openalex_id,
                },
                "wikipedia_url": self.wikipedia_url,
                # "image_url": self.image_url,
                # "image_thumbnail_url": self.image_thumbnail_url,
                # "international": {
                #     "display_name": self.display_name_international,
                #     "description": self.description_international
                # },
                # "counts_by_year": self.display_counts_by_year,
                # "works_api_url": f"https://api.openalex.org/works?filter=concepts.id:{self.openalex_id_short}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })

            # only include non-null IDs
            for id_type in list(response["ids"].keys()):
                if response["ids"][id_type] == None:
                    del response["ids"][id_type]
        return response

    def __repr__(self):
        return "<Topic ( {} ) {} {}>".format(self.openalex_api_url, self.id, self.display_name)


logger.info(f"loading valid topic IDs")
_valid_topics = db.session.query(Topic.topic_id).options(orm.Load(Topic).raiseload('*')).all()
_valid_topic_ids = set([t.topic_id for t in _valid_topics])


def is_valid_topic_id(topic_id):
    return topic_id and topic_id in _valid_topic_ids


# class TopicJsonEntityHash(db.Model):
#     __table_args__ = {'schema': 'mid'}
#     __tablename__ = "topic_json_entity_hash"

#     topic_id = db.Column(
#         db.Integer, db.ForeignKey("mid.topics"), primary_key=True)
#     json_entity_hash = db.Column(db.Text)


# Topic.json_entity_hash = db.relationship(
#     TopicJsonEntityHash, lazy='selectin', uselist=False
# )
