import datetime
import json

from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import ARRAY

from app import MAX_MAG_ID
from app import db
from app import get_apiurl_from_openalex_url
from app import logger
from util import entity_md5
from util import truncate_on_word_break


# truncate mid.journal
# insert into mid.journal (select * from legacy.mag_main_journals)
# update mid.journal set display_title=replace(display_title, '\\\\/', '/');
# update mid.journal set publisher=replace(publisher, '\t', '') where publisher ~ '\t';

def as_source_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/S{id}"


class Source(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "journal"

    journal_id = db.Column(db.BigInteger, primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    issn = db.Column(db.Text)
    issns = db.Column(db.Text)
    issns_text_array = db.Column(ARRAY(db.Text))
    is_oa = db.Column(db.Boolean)
    is_in_doaj = db.Column(db.Boolean)
    publisher = db.Column(db.Text)
    publisher_id = db.Column(db.BigInteger, db.ForeignKey('mid.publisher.publisher_id'))
    institution_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"))
    normalized_book_publisher = db.Column(db.Text)
    normalized_conference = db.Column(db.Text)
    webpage = db.Column(db.Text)
    repository_id = db.Column(db.Text)
    type = db.Column(db.Text)
    apc_prices = db.Column(JSONB)
    apc_usd = db.Column(JSONB)
    is_society_journal = db.Column(db.Boolean)
    societies = db.Column(JSONB)
    alternate_titles = db.Column(JSONB)
    abbreviated_title = db.Column(db.Text)
    country_code = db.Column(db.Text)
    image_url = db.Column(db.Text)
    image_thumbnail_url = db.Column(db.Text)
    fatcat_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)
    full_updated_date = db.Column(db.DateTime)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)
    json_entity_hash = db.Column(db.Text)

    @property
    def openalex_id(self):
        return as_source_openalex_id(self.journal_id)

    @property
    def id(self):
        return self.journal_id

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    @property
    def issn_l(self):
        return self.issn

    def store(self):
        index_record = None
        entity_hash = None

        if self.merge_into_id is not None:
            entity_hash = entity_md5(self.merge_into_id)

            if entity_hash != self.json_entity_hash:
                logger.info(f"merging {self.openalex_id} into {self.merge_into_id}")
                index_record = {
                    "_index": "merge-sources",
                    "_id": self.openalex_id,
                    "_source": {
                        "id": self.openalex_id,
                        "merge_into_id": as_source_openalex_id(self.merge_into_id),
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
                    "_index": "sources-v2",
                    "_id": self.openalex_id,
                    "_source": my_dict
                }
            else:
                logger.info(f"dictionary not changed, don't save again {self.openalex_id}")

        self.json_entity_hash = entity_hash
        return index_record

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
    def concepts(self):
        from models.concept import as_concept_openalex_id
        response = []
        if self.counts and self.counts.paper_count:
            q = """
                select ancestor_id as id, concept.wikidata_id as wikidata, ancestor_name as display_name,
                ancestor_level as level,
                round(100 * (0.0+count(distinct wc.paper_id))/counts.paper_count, 1)::float as score
                from mid.journal journal
                join mid.citation_journals_mv counts on counts.journal_id=journal.journal_id
                join mid.work work on work.journal_id=journal.journal_id
                join mid.work_concept wc on wc.paper_id=work.paper_id
                join mid.concept_self_and_ancestors_mv ancestors on ancestors.id=wc.field_of_study
                join mid.concept_for_api_mv concept on concept.field_of_study_id=ancestor_id
                where journal.journal_id=:journal_id
                group by ancestor_id, concept.wikidata_id, ancestor_name, ancestor_level, counts.paper_count
                order by score desc
                """
            rows = db.session.execute(text(q), {"journal_id": self.journal_id}).fetchall()
            response = [dict(row) for row in rows if row["score"] and row["score"] > 20]
            for row in response:
                row["id"] = as_concept_openalex_id(row["id"])
        return response

    @classmethod
    def to_dict_null_minimum(self):
        response = {
            "id": None,
            "issn_l": None,
            "issn": None,
            "display_name": None,
            "publisher": None,
            "type": None,
        }
        return response

    @property
    def publisher_display_name(self):
        if self.publisher_entity:
            return self.publisher_entity.display_name
        else:
            return self.publisher

    @property
    def host_organization(self):
        if self.type == "journal" and self.publisher_entity:
            return self.publisher_entity
        elif self.type == "repository" and self.institution:
            return self.institution
        else:
            return None

    @property
    def host_organization_name(self):
        return self.host_organization.display_name if self.host_organization else None

    @property
    def host_organization_id(self):
        if self.host_organization and self.host_organization.openalex_id:
            return self.host_organization.openalex_id
        else:
            return None

    def host_organization_lineage(self):
        if self.type == "journal" and self.publisher_entity:
            return self.publisher_entity.lineage()
        elif self.type == "repository" and self.institution:
            return [self.institution.openalex_id]
        else:
            return []

    @property
    def host_institution_lineage(self):
        if self.type == "repository" and self.institution:
            return [self.institution.openalex_id]
        else:
            return []

    @property
    def host_institution_lineage_names(self):
        if self.type == "repository" and self.institution:
            return [self.institution.display_name]
        else:
            return []

    @property
    def publisher_lineage(self):
        if self.publisher_entity and self.type != "repository":
            return self.publisher_entity.lineage()
        else:
            return []

    @property
    def publisher_lineage_names(self):
        if self.publisher_entity and self.type != "repository":
            return self.publisher_entity.lineage_names()
        else:
            return []

    def host_organization_lineage_names(self):
        if self.type == "journal" and self.publisher_entity:
            return self.publisher_entity.lineage_names()
        elif self.type == "repository" and self.institution:
            return [self.institution.display_name]
        else:
            return []

    def oa_percent(self):
        if not (self.counts and self.counts.paper_count and self.counts.oa_paper_count):
            return 0

        return min(round(100.0 * float(self.counts.oa_paper_count) / float(self.counts.paper_count), 2), 100)

    @property
    def apc_prices_with_0(self):
        if self.apc_prices:
            return self.apc_prices
        elif self.is_in_doaj:
            # in DOAJ but no APC, which means APC is 0
            return [{"price": 0, "currency": "USD"}]
        else:
            return None

    @property
    def apc_usd_with_0(self):
        if self.apc_usd:
            return self.apc_usd
        elif self.is_in_doaj and not self.apc_prices:
            # in DOAJ but no APC, which means APC is 0
            return 0
        else:
            return None

    def to_dict(self, return_level="full", check_merge=True):
        if check_merge and self.merge_into_id and self.merged_into_source:
            return self.merged_into_source.to_dict(return_level=return_level, check_merge=False)

        response = {
            "id": self.openalex_id,
            "issn_l": self.issn,
            "issn": json.loads(self.issns) if self.issns else None,
            "display_name": truncate_on_word_break(self.display_name, 500),
            "publisher": self.publisher_display_name,
            "host_organization": self.host_organization_id,
            "host_organization_name": self.host_organization_name,
            "host_organization_lineage": self.host_organization_lineage(),
            "host_organization_lineage_names": self.host_organization_lineage_names(),
            "is_in_doaj": self.is_in_doaj or False,
            "host_institution_lineage": self.host_institution_lineage,
            "host_institution_lineage_names": self.host_institution_lineage_names,
            "publisher_lineage": self.publisher_lineage,
            "publisher_lineage_names": self.publisher_lineage_names,
            "publisher_id": self.publisher_entity and self.publisher_entity.openalex_id,
            "type": self.type,
        }
        if return_level == "full":
            response.update({
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
                "is_oa": self.is_oa or False,
                "alternate_titles": self.alternate_titles,
                "abbreviated_title": self.abbreviated_title,
                "homepage_url": self.webpage,
                "country_code": self.country_code,
                "ids": {
                    "openalex": self.openalex_id,
                    "issn_l": self.issn,
                    "issn": json.loads(self.issns) if ((self.issns) and (self.issns != '[]')) else None,
                    "mag": self.journal_id if self.journal_id < MAX_MAG_ID else None,
                    "fatcat": self.fatcat_id,
                    "wikidata": self.wikidata_id
                },
                "apc_prices": self.apc_prices_with_0,
                "apc_usd": self.apc_usd_with_0,
                "societies": self.societies,
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts[0:25],
                "works_api_url": f"https://api.openalex.org/works?filter=primary_location.source.id:{self.openalex_id_short}",
                "updated_date": datetime.datetime.utcnow().isoformat()[0:10],
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })

            # only include non-null IDs
            for id_type in list(response["ids"].keys()):
                if response["ids"][id_type] == None:
                    del response["ids"][id_type]
        return response



    def __repr__(self):
        return "<Source ( {} ) {} {}>".format(self.openalex_api_url, self.id, self.display_name)

# select count(distinct work.paper_id)
# from mid.journal journal
# join mid.work work on work.journal_id=journal.journal_id
# where issn='0138-9130' -- peerjissn='2167-8359'
#
# select ancestor_level, ancestor_name, count(distinct work.paper_id) as n, count(distinct work.paper_id)/6599.0 as prop
# from mid.journal journal
# join mid.work work on work.journal_id=journal.journal_id
# join mid.work_concept wc on wc.paper_id=work.paper_id
# join mid.concept concept on concept.field_of_study_id=wc.field_of_study
# join mid.concept_self_and_ancestors_view ancestors on ancestors.id=concept.field_of_study_id
# where issn='0138-9130' -- peerjissn='2167-8359'
# group by ancestor_name, ancestor_level
# order by n desc

