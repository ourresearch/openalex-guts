import datetime
import json

from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

from app import db
from app import logger
from util import dictionary_nested_diff, entity_md5
from util import jsonify_fast_no_sort_raw


def as_publisher_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/P{id}"


class Publisher(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher"

    publisher_id = db.Column(db.BigInteger, primary_key=True)
    created_date = db.Column(db.DateTime)
    display_name = db.Column(db.Text)
    alternate_titles = db.Column(JSONB)
    country_codes = db.Column(JSONB)
    parent_publisher = db.Column(db.BigInteger, db.ForeignKey('mid.publisher.publisher_id'))
    hierarchy_level = db.Column(db.Integer)
    ror_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    homepage_url = db.Column(db.Text)
    image_url = db.Column(db.Text)
    image_thumbnail_url = db.Column(db.Text)
    country_name = db.Column(db.Text)
    is_approved = db.Column(db.Boolean)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)
    json_entity_hash = db.Column(db.Text)

    @property
    def openalex_id(self):
        return as_publisher_openalex_id(self.publisher_id)

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
                    'role': 'publisher',
                    'id': self.openalex_id,
                    'works_count': int(self.counts.paper_count or 0) if self.counts else 0,
                })
            else:
                from models import hydrate_role
                e = hydrate_role(entity_id)
                if e is not None:
                    response.append(hydrate_role(entity_id))

        # there can be duplicate funders
        # quick fix for now: only keep the funder with the highest works_count
        funders = [e for e in response if e['role'] == 'funder']
        if funders and len(funders) > 1:
            id_to_keep = max(funders, key=lambda x: x['works_count'])['id']
            response = [e for e in response if e['id'] == id_to_keep or e['role'] != 'funder']
        return response

    def lineage(self):
        return [f"https://openalex.org/P{p.ancestor_id}" for p in self.self_and_ancestors]

    def lineage_names(self):
        return [p.ancestor_display_name for p in self.self_and_ancestors]

    def oa_percent(self):
        if not (self.counts and self.counts.paper_count and self.counts.oa_paper_count):
            return 0

        return min(round(100.0 * float(self.counts.oa_paper_count) / float(self.counts.paper_count), 2), 100)

    @cached_property
    def concepts(self):
        from models.concept import as_concept_openalex_id

        if not self.counts or not self.counts.paper_count:
            return []

        q = """
               select concept_id as id, wikidata, display_name, level, score
               from mid.publisher_concepts_mv
               where publisher_id=:publisher_id
               order by score desc
            """

        rows = db.session.execute(text(q), {"publisher_id": self.publisher_id}).fetchall()
        response = [dict(row) for row in rows if row["score"] and row["score"] > 20]
        for row in response:
            row["id"] = as_concept_openalex_id(row["id"])
        return response

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "display_name": self.display_name
        }
        if return_level == "full":
            response.update({
                "ids": {
                    "openalex": self.openalex_id,
                    "wikidata": self.wikidata_id,
                    "ror": self.ror_id,
                },
                "alternate_titles": self.alternate_titles or [],
                "parent_publisher": self.parent and self.parent.to_dict(return_level="minimal"),
                "lineage": self.lineage(),
                "hierarchy_level": self.hierarchy_level,
                "country_codes": self.country_codes or [],
                "homepage_url": self.homepage_url,
                "image_url": self.image_url,
                "image_thumbnail_url": self.image_thumbnail_url,
                "roles": self.roles,
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "sources_count": int(self.sources_count.num_sources or 0) if self.sources_count else 0,
                "summary_stats": {
                    "2yr_mean_citedness": (self.impact_factor and self.impact_factor.impact_factor) or 0,
                    "h_index": (self.h_index and self.h_index.h_index) or 0,
                    "i10_index": (self.i10_index and self.i10_index.i10_index) or 0,
                    "oa_percent": self.oa_percent(),
                    "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                    "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                    "sources_count": int(self.sources_count.num_sources or 0) if self.sources_count else 0,
                    "2yr_works_count": int(self.counts_2year.paper_count or 0) if self.counts_2year else 0,
                    "2yr_cited_by_count": int(self.counts_2year.citation_count or 0) if self.counts_2year else 0,
                    "2yr_i10_index": int(self.i10_index_2year.i10_index or 0) if self.i10_index_2year else 0,
                    "2yr_h_index": int(self.h_index_2year.h_index or 0) if self.h_index_2year else 0
                },
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts[0:25],
                "sources_api_url": f"https://api.openalex.org/sources?filter=host_organization.id:{self.openalex_id_short}",
                "updated_date": datetime.datetime.utcnow().isoformat()[0:10],
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })

            # only include non-null IDs
            for id_type in list(response["ids"].keys()):
                if response["ids"][id_type] is None:
                    del response["ids"][id_type]

        return response

    def store(self):
        index_record = None
        entity_hash = None

        if self.merge_into_id is not None:
            entity_hash = entity_md5(self.merge_into_id)

            if entity_hash != self.json_entity_hash:
                logger.info(f"merging {self.openalex_id} into {self.merge_into_id}")
                index_record = {
                    "_index": "merge-publishers",
                    "_id": self.openalex_id,
                    "_source": {
                        "id": self.openalex_id,
                        "merge_into_id": as_publisher_openalex_id(self.merge_into_id),
                    }
                }
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
                    "_index": "publishers-v4",
                    "_id": self.openalex_id,
                    "_source": my_dict
                }
            else:
                logger.info(f"dictionary not changed, don't save again {self.openalex_id}")

        self.json_entity_hash = entity_hash
        return index_record


class PublisherSelfAndAncestors(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher_self_and_ancestors_mv"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True)
    display_name = db.Column(db.Text)
    ancestor_id = db.Column(db.BigInteger, primary_key=True)
    ancestor_display_name = db.Column(db.Text)
    ancestor_hierarchy_level = db.Column(db.Integer)


class PublisherSources(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher_sources_mv"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True)
    num_sources = db.Column(db.Integer)
