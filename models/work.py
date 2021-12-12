from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
import datetime
from collections import defaultdict

import shortuuid
import random


from app import db
from util import normalize_title
from util import jsonify_fast_no_sort_raw


# truncate mid.work
# insert into mid.work (select * from legacy.mag_main_papers)
# update mid.work set original_title=replace(original_title, '\\\\/', '/');

# update work set match_title = f_matching_string(original_title)

class Work(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work"

    # id = db.Column(db.BigInteger)
    paper_id = db.Column(db.BigInteger, primary_key=True)
    doi = db.Column(db.Text)
    doc_type = db.Column(db.Text)
    paper_title = db.Column(db.Text)
    original_title = db.Column(db.Text)
    year = db.Column(db.Numeric)
    publication_date = db.Column(db.DateTime)
    online_date = db.Column(db.DateTime)
    publisher = db.Column(db.Text)
    journal_id = db.Column(db.BigInteger)
    volume = db.Column(db.Text)
    issue = db.Column(db.Text)
    first_page = db.Column(db.Text)
    last_page = db.Column(db.Text)
    reference_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    estimated_citation = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)
    doi_lower = db.Column(db.Text)
    doc_sub_types = db.Column(db.Text)
    genre = db.Column(db.Text)
    is_paratext = db.Column(db.Boolean)
    oa_status = db.Column(db.Text)
    best_url = db.Column(db.Text)
    best_free_url = db.Column(db.Text)
    best_free_version = db.Column(db.Text)

    match_title = db.Column(db.Text)

    # queues
    started = db.Column(db.DateTime)
    finished = db.Column(db.DateTime)
    started_label = db.Column(db.Text)


    def __init__(self, **kwargs):
        self.id = random.randint(1000, 10000000)
        self.paper_id = self.id
        self.created = datetime.datetime.utcnow().isoformat()
        self.updated = self.created
        super(Work, self).__init__(**kwargs)

    @property
    def id(self):
        return self.paper_id

    def refresh(self):
        print("refreshing! {}".format(self.id))
        self.title = self.records[0].title

        # build citations list (combine crossref + pubmed via some way, look up IDs)
        # build concept list (call concept API)
        # build author list
        # build institution list
        # build easy metadata (abstract, mesh, etc)
        # extract paper urls
        # maybe
        # assign paper recommendations, PaperExtendedAttributes, etc

        self.updated = datetime.datetime.utcnow().isoformat()
        print("done! {}".format(self.id))

    @cached_property
    def is_retracted(self):
        if self.doc_sub_types != None:
            return True
        return False

    @cached_property
    def affiliations_sorted(self):
        return sorted(self.affiliations, key=lambda x: x.author_sequence_number)

    @cached_property
    def affiliations_list(self):
        affiliations = [affiliation for affiliation in self.affiliations_sorted[:100]]
        last_author_sequence_number = max([affil.author_sequence_number for affil in affiliations])
        affiliation_dict = defaultdict(list)
        for affil in affiliations:
            affil.author_position = "middle"
            if affil.author_sequence_number == 1:
                affil.author_position = "first"
            elif affil.author_sequence_number == last_author_sequence_number:
                affil.author_position = "last"
            affiliation_dict[affil.author_sequence_number] += [affil.to_dict("minimum")]
        response = []
        for seq, affil_list in affiliation_dict.items():
            response_dict = {"author_position": affil_list[0]["author_position"],
                             "author": affil_list[0]["author"],
                             "institutions": [a["institution"] for a in affil_list],
                     }
            response.append(response_dict)
        return response

    @property
    def concepts_sorted(self):
        return sorted(self.concepts, key=lambda x: x.score, reverse=True)

    @property
    def locations_sorted(self):
        return sorted(self.locations, key=lambda x: (x.is_oa == True, x.source_url), reverse=True)

    @property
    def mag_publisher(self):
        return self.publisher

    @property
    def work_title(self):
        return self.original_title

    @property
    def work_id(self):
        return self.paper_id

    @property
    def doi_url(self):
        if not self.doi:
            return None
        return "https://doi.org/{}".format(self.doi_lower)

    @cached_property
    def references_list(self):
        import models

        reference_paper_ids = [reference.paper_reference_id for reference in self.references]
        return reference_paper_ids

        # objs = db.session.query(Work).options(
        #      selectinload(Work.journal).selectinload(models.Venue.journalsdb),
        #      selectinload(Work.extra_ids),
        #      selectinload(Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids),
        #      selectinload(Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror),
        #      orm.Load(Work).raiseload('*')).filter(Work.paper_id.in_(reference_paper_ids)).all()
        # response = [obj.to_dict("minimum") for obj in objs]
        # return response

    @cached_property
    def related_paper_list(self):
        import models
        q = """
        select recommended_paper_id as id from legacy.mag_advanced_paper_recommendations WHERE paper_id = :paper_id order by score desc
        """
        rows = db.session.execute(text(q), {"paper_id": self.paper_id}).fetchall()
        related_paper_ids = [row[0] for row in rows]
        return related_paper_ids

        # objs = db.session.query(Work).options(
        #      selectinload(Work.journal).selectinload(models.Venue.journalsdb),
        #      selectinload(Work.extra_ids),
        #      selectinload(Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids),
        #      selectinload(Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror),
        #      orm.Load(Work).raiseload('*')).filter(Work.paper_id.in_(related_paper_ids)).all()
        # response = [obj.to_dict("minimum") for obj in objs]
        # return response

    def process(self):
        VERSION_STRING = "full dict, no citations"

        # print("processing work! {}".format(self.id))
        self.json_elastic = jsonify_fast_no_sort_raw(self.to_dict())

        # has to match order of get_insert_fieldnames
        json_elastic_escaped = self.json_elastic.replace("'", "''").replace("%", "%%").replace(":", "\:")
        if len(json_elastic_escaped) > 65000:
            print("Error: json_elastic_escaped too long for paper_id {}, skipping".format(self.work_id))
            json_elastic_escaped = None
        self.insert_dict = {"mid.work_json": "({paper_id}, '{updated}', '{json_elastic}', '{version}')".format(
                                                                  paper_id=self.paper_id,
                                                                  updated=datetime.datetime.utcnow().isoformat(),
                                                                  json_elastic=json_elastic_escaped,
                                                                  version=VERSION_STRING
                                                                )}

        # print(self.json_elastic[0:100])

    def get_insert_fieldnames(self, table_name=None):
        lookup = {
            "mid.work_json": ["paper_id", "updated", "json_elastic", "version"]
        }
        if table_name:
            return lookup[table_name]
        return lookup

    def to_dict(self, return_level="full"):
        response = {
            "id": self.work_id,
            "paper_title": self.work_title,
            "publication_year": self.year,
            "publication_date": self.publication_date,
            "external_ids": {},
            "genre": self.genre,
            "best_url": self.best_url,
            "oa_status": self.oa_status,
            "best_free_url": self.best_free_url,
            "venue": self.journal.to_dict("minimum") if self.journal else None,
            "author_institutions": self.affiliations_list,
        }
        if self.doi:
            response["external_ids"]["doi"] = self.doi_url
        if self.extra_ids:
            for extra_id in self.extra_ids:
                response["external_ids"][extra_id.id_type] = extra_id.url

        if return_level == "full":
            response.update({
            # "doc_type": self.doc_type,
            "is_retracted": self.is_retracted,
            "is_paratext": self.is_paratext,
            "best_url": self.best_url,
            "oa_status": self.oa_status,
            "best_free_url": self.best_free_url,
            "best_free_version": self.best_free_version,
            "volume": self.volume,
            "issue": self.issue,
            "first_page": self.first_page,
            "last_page": self.last_page,
            "references_count": self.reference_count,
            "cited_by_count": self.citation_count,
            "concepts": [concept.to_dict("minimum") for concept in self.concepts_sorted],
            "mesh": [mesh.to_dict("minimum") for mesh in self.mesh],
            "alternate_locations": [location.to_dict("minimum") for location in self.locations_sorted if location.is_oa == True],
            "referenced_works": self.references_list,
            "related_works": self.related_paper_list,
            "abstract_inverted_index": self.abstract.to_dict("minimum") if self.abstract else None,
            "cited_by_api_url": f"https://elastic.api.openalex.org/works?filter=cites:{self.paper_id}&details=true",
            "updated_date": self.updated_date,
        })
        return response


    def __repr__(self):
        return "<Work ( {} ) {} '{}...'>".format(self.paper_id, self.doi, self.paper_title[0:20])



