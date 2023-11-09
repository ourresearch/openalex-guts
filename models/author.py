import datetime
import json
import re

from cached_property import cached_property
from sqlalchemy import orm

from app import MAX_MAG_ID
from app import db
from app import get_apiurl_from_openalex_url
from app import get_db_cursor
from app import logger
from util import entity_md5
from util import truncate_on_word_break

DELETED_AUTHOR_ID = 5317838346


def as_author_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/A{id}"


class Author(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author"

    author_id = db.Column(db.BigInteger, primary_key=True)
    display_name = db.Column(db.Text)
    last_known_affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"))
    last_known_affiliation_id_date = db.Column(db.DateTime)
    match_name = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)
    full_updated_date = db.Column(db.DateTime)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)
    json_entity_hash = db.Column(db.Text)

    @property
    def id(self):
        return self.author_id

    @classmethod
    def matching_author_string(cls, raw_author_string):
        from util import matching_author_string
        if not raw_author_string:
            return None

        return matching_author_string(raw_author_string)

    @classmethod
    def try_to_match(cls, raw_author_string, original_orcid, citation_paper_ids):
        if not raw_author_string and not original_orcid:
            return None

        match_with_orcid = f"""
            select author_id from mid.author_orcid
            join mid.author using (author_id)
            where author_orcid.orcid = '{original_orcid}'
            and author.merge_into_id is null
            """

        match_name = Author.matching_author_string(raw_author_string)
        if len(citation_paper_ids) > 1:
            citation_paper_ids_tuple = tuple(citation_paper_ids)
        else:
            # doesn't parse into sql properly if only one, so duplicate it for no harm
            citation_paper_ids_tuple = tuple(citation_paper_ids + citation_paper_ids)
        match_with_citations = f"""
            select affil.author_id 
            from mid.author author
            join mid.affiliation affil on affil.author_id=author.author_id
            where author.author_id is not null
            and author.merge_into_id is null
            and affil.paper_id in {citation_paper_ids_tuple}
            and author.match_name = '{match_name}'
            """

        with get_db_cursor() as cur:
            if original_orcid:
                cur.execute(match_with_orcid)
                rows = cur.fetchall()
                if rows:
                    logger.info(f"matched: author using orcid")
                    return Author.query.options(orm.Load(Author).raiseload('*')).get(rows[0]["author_id"])
            if citation_paper_ids:
                cur.execute(match_with_citations)
                rows = cur.fetchall()
                if rows:
                    logger.info(f"matched: author using citations")
                    return Author.query.options(orm.Load(Author).raiseload('*')).get(rows[0]["author_id"])
        return None


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
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    @property
    def last_known_institution_api_url(self):
        if not self.last_known_affiliation_id:
            return None
        return f"http://localhost:5007/institution/id/{self.last_known_affiliation_id}"

    @property
    def orcid_object(self):
        # eventually get it like this from ORCID api:  curl -vLH'Accept: application/json' https://orcid.org/0000-0003-0902-4386
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

    def store(self):
        bulk_actions = []

        if self.merge_into_id is not None:
            entity_hash = entity_md5(self.merge_into_id)

            if entity_hash != self.json_entity_hash:
                logger.info(f"merging {self.openalex_id} into {self.merge_into_id}")
                index_record = {
                    "_op_type": "index",
                    "_index": "merge-authors",
                    "_id": self.openalex_id,
                    "_source": {
                        "id": self.openalex_id,
                        "merge_into_id": as_author_openalex_id(self.merge_into_id),
                    }
                }
                delete_record = {
                    "_op_type": "delete",
                    "_index": "authors-v12",
                    "_id": self.openalex_id,
                }
                bulk_actions.append(index_record)
                bulk_actions.append(delete_record)

            else:
                logger.info(f"already merged into {self.merge_into_id}, not saving again")
        else:
            my_dict = self.to_dict()
            my_dict['updated'] = my_dict.get('updated_date')
            my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
            my_dict['@version'] = 1
            entity_hash = entity_md5(my_dict)

            if entity_hash != self.json_entity_hash:
                logger.info(f"dictionary for {self.openalex_id} new or changed, so save again")
                index_record = {
                    "_op_type": "index",
                    "_index": "authors-v12",
                    "_id": self.openalex_id,
                    "_source": my_dict
                }
                bulk_actions.append(index_record)
            else:
                logger.info(f"dictionary not changed, don't save again {self.openalex_id}")

        self.json_entity_hash = entity_hash
        return bulk_actions

    @cached_property
    def concepts(self):
        if not self.author_concepts:
            return []
        response = [author_concept.to_dict() for author_concept in self.author_concepts if author_concept.score and author_concept.score > 20]
        response = sorted(response, key=lambda x: x["score"], reverse=True)
        return response

    @cached_property
    def display_counts_by_year(self):
        response_dict = {}
        all_rows = self.counts_by_year_papers + self.counts_by_year_oa_papers + self.counts_by_year_citations
        for count_row in all_rows:
            response_dict[count_row.year] = {
                "year": count_row.year,
                "works_count": 0,
                "oa_works_count": 0,
                "cited_by_count": 0
            }
        for count_row in all_rows:
            if count_row.type == "citation_count":
                response_dict[count_row.year]["cited_by_count"] = int(count_row.n)
            elif count_row.type == "oa_works_count":
                response_dict[count_row.year]["oa_works_count"] = int(count_row.n)
            else:
                response_dict[count_row.year]["works_count"] = int(count_row.n)

        my_dicts = [counts for counts in response_dict.values() if counts["year"] and counts["year"] >= 2012]
        response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
        return response

    @cached_property
    def most_cited_work_string(self):
        my_works = [a.work for a in self.affiliations if a.work and a.work.counts]
        if my_works:
            most_cited_work = sorted(my_works, key=lambda w: w.counts.citation_count, reverse=True)[0]
        else:
            return None

        if not most_cited_work.work_title:
            return None

        title_words = re.split(r'\s+', most_cited_work.work_title)
        if len(title_words) < 20:
            title_str = most_cited_work.work_title
        else:
            title_str = ' '.join(title_words[0:20]) + ' ...'

        if most_cited_work.year:
            title_str += f' ({most_cited_work.year})'

        return title_str

    def oa_percent(self):
        if not (self.counts and self.counts.paper_count and self.counts.oa_paper_count):
            return 0

        return min(round(100.0 * float(self.counts.oa_paper_count) / float(self.counts.paper_count), 2), 100)

    def to_dict(self, return_level="full"):
        response = {
                "id": self.openalex_id,
                "orcid": self.orcid_url,
                "display_name": truncate_on_word_break(self.display_name, 100),
              }
        if return_level == "full":
            response.update({
                "display_name_alternatives": [truncate_on_word_break(n, 100) for n in self.all_alternative_names],
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "most_cited_work": self.most_cited_work_string,
                "summary_stats": {
                    "2yr_mean_citedness": (self.impact_factor and self.impact_factor.impact_factor) or 0,
                    "h_index": (self.h_index and self.h_index.h_index) or 0,
                    "i10_index": (self.i10_index and self.i10_index.i10_index) or 0,
                    "oa_percent": self.oa_percent(),
                    "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                    "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                    "2yr_works_count": int(self.counts_2year.paper_count or 0) if self.counts_2year else 0,
                    "2yr_cited_by_count": int(self.counts_2year.citation_count or 0) if self.counts_2year else 0,
                    "2yr_i10_index": int(self.i10_index_2year.i10_index or 0) if self.i10_index_2year else 0,
                    "2yr_h_index": int(self.h_index_2year.h_index or 0) if self.h_index_2year else 0
                },
                "ids": {
                    "openalex": self.openalex_id,
                    "orcid": self.orcid_url,
                    "scopus": self.scopus_url,
                    "twitter": self.twitter_url,
                    "wikipedia": self.wikipedia_url,
                    "mag": self.author_id if self.author_id < MAX_MAG_ID else None
                },
                "last_known_institution": self.last_known_institution.to_dict("minimum") if self.last_known_institution else None,
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts[0:25],
                "works_api_url": f"https://api.openalex.org/works?filter=author.id:{self.openalex_id_short}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })

            # only include non-null IDs
            for id_type in list(response["ids"].keys()):
                if response["ids"][id_type] == None:
                    del response["ids"][id_type]
        return response

    def __repr__(self):
        return "<Author ( {} ) {} {}>".format(self.openalex_api_url, self.id, self.display_name)
