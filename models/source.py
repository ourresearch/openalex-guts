import datetime
import json

from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

from app import MAX_MAG_ID
from app import db
from app import get_apiurl_from_openalex_url
from app import logger
from util import dictionary_nested_diff
from util import jsonify_fast_no_sort_raw
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
    is_oa = db.Column(db.Boolean)
    is_in_doaj = db.Column(db.Boolean)
    publisher = db.Column(db.Text)
    publisher_id = db.Column(db.BigInteger, db.ForeignKey('mid.publisher.publisher_id'))
    institution_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"))
    normalized_book_publisher = db.Column(db.Text)
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
    fatcat_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)
    full_updated_date = db.Column(db.DateTime)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)

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
                logger.debug(f"Source JSON Diff: {diff}")

        now = datetime.datetime.utcnow().isoformat()
        self.full_updated_date = now
        my_dict["updated_date"] = now

        json_save = None
        if not self.merge_into_id:
            json_save = jsonify_fast_no_sort_raw(my_dict)

        venues_json_save = json_save
        if venues_json_save:
            venues_json_save = venues_json_save.replace('https://openalex.org/S', 'https://openalex.org/V')

        self.insert_dicts = [
            {
                "JsonVenues": {
                    "id": self.journal_id,
                    "updated": now,
                    "changed": now,
                    "json_save": venues_json_save,
                    "version": VERSION_STRING,
                    "merge_into_id": self.merge_into_id
                }
            },
            {
                "JsonSources": {
                    "id": self.journal_id,
                    "updated": now,
                    "changed": now,
                    "json_save": json_save,
                    "version": VERSION_STRING,
                    "merge_into_id": self.merge_into_id
                }
            }
        ]



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
        if self.type == "journal" and self.publisher_entity and self.publisher_entity.openalex_id:
            return self.publisher_entity.openalex_id
        elif self.type == "repository" and self.institution and self.institution.openalex_id:
            return self.institution.openalex_id
        else:
            return None

    def to_dict(self, return_level="full"):
        response = {
            "id": self.openalex_id,
            "issn_l": self.issn,
            "issn": json.loads(self.issns) if self.issns else None,
            "display_name": truncate_on_word_break(self.display_name, 500),
            "publisher": self.publisher_display_name,
            "host_organization": self.host_organization,
            "host_organization_name": self.publisher_display_name,
            "publisher_id": self.publisher_entity and self.publisher_entity.openalex_id,
            "type": self.type,
        }
        if return_level == "full":
            response.update({
                "works_count": self.counts.paper_count if self.counts else 0,
                "cited_by_count": self.counts.citation_count if self.counts else 0,
                "summary_stats": {
                    "2yr_mean_citedness": (self.impact_factor and self.impact_factor.impact_factor) or 0,
                    "h_index": (self.h_index and self.h_index.h_index) or 0,
                    "i10_index": (self.i10_index and self.i10_index.i10_index) or 0
                },
                "is_oa": self.is_oa or False,
                "is_in_doaj": self.is_in_doaj or False,
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
                "apc_usd": self.apc_usd,
                "societies": self.societies,
                "counts_by_year": self.display_counts_by_year,
                "x_concepts": self.concepts[0:25],
                "works_api_url": f"https://api.openalex.org/works?filter=host_venue.id:{self.openalex_id_short}",
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

