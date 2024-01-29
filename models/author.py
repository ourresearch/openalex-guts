import datetime
import json
import re

from cached_property import cached_property

from app import AUTHORS_INDEX
from app import MAX_MAG_ID
from app import db
from app import get_apiurl_from_openalex_url
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
    def matching_author_strings(cls, raw_author_string):
        from util import matching_author_strings
        if not raw_author_string:
            return None

        return matching_author_strings(raw_author_string)

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
                    "_index": "merge-authors-v1",
                    "_id": self.openalex_id,
                    "_source": {
                        "id": self.openalex_id,
                        "merge_into_id": as_author_openalex_id(self.merge_into_id),
                    }
                }
                delete_record = {
                    "_op_type": "delete",
                    "_index": AUTHORS_INDEX,
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
                    "_index": AUTHORS_INDEX,
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

    def last_known_affiliation_override(self):
        # quick fix implemented for Sorbonne University
        # checks most recent paper's affiliations, and if Sorbonne is one of them, force it to be the last known institution
        override_affiliation_ids = [39804081]  # just sorbonne for now, but more can be added
        affils = sorted(
            self.affiliations,
            key=lambda x: x.work.publication_date if x.work.publication_date is not None else '1800-01-01',  # set null date as old
            reverse=True
        )
        affils_most_recent_work = [af.affiliation_id for af in affils if af.paper_id == affils[0].paper_id]
        for affiliation_id in override_affiliation_ids:
            if affiliation_id in affils_most_recent_work:
                print(f"setting last_known_affiliation_id to {affiliation_id}")
                self.last_known_affiliation_id = affiliation_id
                db.session.add(self)
                db.session.commit()
                return

    @cached_property
    def last_known_institutions(self):
        """
        last_known_institutions will be a list of institutions, to handle the cases where the last work has multiple affiliations
        We will deprecate last_known_institution, and use this instead
        """
        # get a list of tuples: (publication_date, affiliation), sorted by publication date, most recent first
        sorted_affiliations = sorted(
            [
                (datetime.datetime.fromisoformat(affil.work.publication_date), affil)
                for affil in self.affiliations
                if affil.affiliation_id is not None and affil.work.merge_into_id is None
            ],
            key=lambda x: x[0],
            reverse=True,
        )
        if not sorted_affiliations:
            return []
        last_affil_ids = set()
        last_known_institutions = []
        max_dt = sorted_affiliations[0][0]
        for dt, affil in sorted_affiliations:
            if dt == max_dt and affil.affiliation_id not in last_affil_ids:
                last_affil_ids.add(affil.affiliation_id)
                if affil.institution:
                    last_known_institutions.append(affil.institution.to_dict("minimum"))
            else:
                break
        return last_known_institutions

    @cached_property
    def affiliations_for_api(self):
        """
        Affiliations field in the API. Different from self.affiliations.
        Includes a list of years for each institution.
        Limited to the first 'max_affiliations' number of unique affiliations.
        """
        max_affiliations = 10
        max_years = 10
        seen_institutions = {}
        formatted_affiliations = []

        for affiliation in self.affiliations:
            if affiliation.affiliation_id is not None and affiliation.work.merge_into_id is None:
                institution = affiliation.institution
                year = affiliation.work.year
                if institution not in seen_institutions and year is not None:
                    seen_institutions[institution] = [year]
                elif year not in seen_institutions[institution] and year is not None:
                    seen_institutions[institution].append(year)

        # sort institutions by the most recent year and limit to max_affiliations
        sorted_institutions = sorted(seen_institutions.items(), key=lambda x: max(x[1]), reverse=True)[
                              :max_affiliations]

        for institution, years in sorted_institutions:
            formatted_affiliations.append(
                {
                    "institution": institution.to_dict("minimum"),
                    "years": sorted(years, reverse=True)[:max_years]
                }
            )

        return formatted_affiliations

    def to_dict(self, return_level="full"):
        response = {
                "id": self.openalex_id,
                "orcid": self.orcid_url,
                "display_name": truncate_on_word_break(self.display_name, 100),
              }
        if return_level == "full":
            self.last_known_affiliation_override()
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
                "affiliations": self.affiliations_for_api,
                "ids": {
                    "openalex": self.openalex_id,
                    "orcid": self.orcid_url,
                    "scopus": self.scopus_url,
                    "twitter": self.twitter_url,
                    "wikipedia": self.wikipedia_url,
                    "mag": self.author_id if self.author_id < MAX_MAG_ID else None
                },
                "last_known_institution": self.last_known_institution.to_dict("minimum") if self.last_known_institution else None,  # we will deprecate this in favor of last_known_institutions
                "last_known_institutions": self.last_known_institutions,
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
