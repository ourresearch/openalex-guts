import datetime
import json

from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

from app import db
from app import logger
from util import dictionary_nested_diff
from util import jsonify_fast_no_sort_raw


def as_funder_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/F{id}"


class Funder(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "funder"

    funder_id = db.Column(db.BigInteger, primary_key=True)
    crossref_id = db.Column(db.BigInteger)
    location = db.Column(db.Text)
    display_name = db.Column(db.Text)
    alternate_titles = db.Column(JSONB)
    country_code = db.Column(db.Text)
    description = db.Column(db.Text)
    homepage_url = db.Column(db.Text)
    image_url = db.Column(db.Text)
    image_thumbnail_url = db.Column(db.Text)
    uri = db.Column(db.Text)
    doi = db.Column(db.Text)
    replaces = db.Column(JSONB)
    replaced_by = db.Column(JSONB)
    tokens = db.Column(JSONB)
    ror_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @property
    def openalex_id(self):
        return as_funder_openalex_id(self.funder_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

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
            elif count_row.type == "oa_paper_count":
                response_dict[count_row.year]["oa_works_count"] = int(count_row.n)
            else:
                response_dict[count_row.year]["works_count"] = int(count_row.n)

        my_dicts = [counts for counts in response_dict.values() if counts["year"] and counts["year"] >= 2012]
        response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
        return response

    @cached_property
    def roles(self):
        q = """
        select id_1, id_2
        from mid.entity_link
        where id_1 = :short_id
        or id_2 = :short_id
        """
        rows = db.session.execute(text(q), {"short_id": self.openalex_id_short}).fetchall()
        entity_ids = set()
        entity_ids.add(self.openalex_id_short)
        for row in rows:
            entity_ids.add(row[0])
            entity_ids.add(row[1])
        response = []
        for entity_id in entity_ids:
            if entity_id == self.openalex_id_short:
                response.append({
                    'role': 'funder',
                    'id': self.openalex_id,
                    'works_count': int(self.counts.paper_count or 0) if self.counts else 0,
                })
            else:
                from models import hydrate_role
                # since there can be multiple funders linked in the link table, we'll throw away any other funders
                # to keep the constraint that there only be one funder in the roles list.
                # there may be a better long-term solution for this
                if entity_id.startswith('F'):
                    continue
                response.append(hydrate_role(entity_id))
        return response

    def oa_percent(self):
        if not (self.counts and self.counts.paper_count and self.counts.oa_paper_count):
            return 0

        return min(round(100.0 * float(self.counts.oa_paper_count) / float(self.counts.paper_count), 2), 100)

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name,
            "ids": {
                "openalex": self.openalex_id,
                "wikidata": self.wikidata_id,
                "ror": self.ror_id,
            },
        }

        if return_level == "full":
            response.update(
                {
                    "alternate_titles": self.alternate_titles or [],
                    "country_code": self.country_code,
                    "description": self.description,
                    "homepage_url": self.homepage_url,
                    "image_url": self.image_url,
                    "image_thumbnail_url": self.image_thumbnail_url,
                    "roles": self.roles,
                    "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                    "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
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
                    "counts_by_year": self.display_counts_by_year,
                    #"sources_api_url": f"https://api.openalex.org/sources?filter=host_organization.id:{self.openalex_id_short}",
                    "updated_date": datetime.datetime.utcnow().isoformat()[0:10],
                    "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
                }
            )

        # only include non-null IDs
        for id_type in list(response["ids"].keys()):
            if response["ids"][id_type] is None:
                del response["ids"][id_type]

        return response

    def store(self):
        VERSION_STRING = "new: updated if changed"
        self.insert_dicts = []
        my_dict = self.to_dict()

        if self.stored and (self.stored.merge_into_id == self.merge_into_id):
            if self.merge_into_id is not None and self.stored.json_save is None:
                #  don't keep saving merged entities and bumping their updated and changed dates
                logger.info(f"already merged into {self.merge_into_id}, not saving again")
                return
            if self.stored.json_save:
                # check merged here for everything but concept
                diff = dictionary_nested_diff(json.loads(self.stored.json_save), my_dict, ["updated_date"])
                if not diff:
                    logger.info(f"dictionary not changed, don't save again {self.openalex_id}")
                    return
                logger.info(f"dictionary for {self.openalex_id} new or changed, so save again")
                logger.debug(f"Funder JSON Diff: {diff}")

        now = datetime.datetime.utcnow().isoformat()
        my_dict["updated_date"] = now

        json_save = None
        if not self.merge_into_id:
            json_save = jsonify_fast_no_sort_raw(my_dict)
        if json_save and len(json_save) > 65000:
            logger.info("Error: json_save too long for funder_id {}, skipping".format(self.openalex_id))
            json_save = None

        self.insert_dicts = [
            {
                "JsonFunders": {
                    "id": self.funder_id,
                    "updated": now,
                    "changed": now,
                    "json_save": json_save,
                    "version": VERSION_STRING,
                    "merge_into_id": self.merge_into_id
                }
            }
        ]


class WorkFunder(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_funder"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    funder_id = db.Column(db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True)
    award = db.Column(JSONB)
