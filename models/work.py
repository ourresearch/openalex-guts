import datetime

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

    @property
    def affiliations_sorted(self):
        return sorted(self.affiliations, key=lambda x: x.author_sequence_number)

    @property
    def concepts_sorted(self):
        return sorted(self.concepts, key=lambda x: x.score, reverse=True)

    @property
    def locations_sorted(self):
        return sorted(self.locations, key=lambda x: (x.source_description, x.source_url))

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
            "year": self.year,
            "publication_date": self.publication_date,
            "doc_type": self.doc_type,
            "genre": None,
            "external_ids": {},
            "volume": self.volume,
            "issue": self.issue,
            "first_page": self.first_page,
            "last_page": self.last_page,
            "reference_count": self.reference_count,
            "cited_by_count": self.citation_count,
            "doc_sub_types": self.doc_sub_types,
            "oa_status": self.oa_status,
            "best_free_version": self.best_free_version,
            "best_free_url": self.best_free_url,
            "best_url": self.best_url,
            "is_paratext": self.is_paratext,
            "genre": self.genre,
            "affiliations": [affiliation.to_dict("minimum") for affiliation in self.affiliations_sorted[:100]],
            "concepts": [concept.to_dict("minimum") for concept in self.concepts_sorted],
            "locations": [location.to_dict("minimum") for location in self.locations_sorted],
            "mesh": [mesh.to_dict("minimum") for mesh in self.mesh],
            "citations": [citation.to_dict("minimum") for citation in self.citations],
            "abstract": self.abstract.to_dict("minimum") if self.abstract else None
        }
        if self.doi:
            response["external_ids"]["doi"] = self.doi_lower
            response["external_ids"]["doi_url"] = self.doi_url
        if self.extra_ids:
            for extra_id in self.extra_ids:
                response["external_ids"][extra_id.id_type] = extra_id.attribute_value
                response["external_ids"][extra_id.id_type + "_url"] = extra_id.url
        if self.journal:
            response["journal"] = self.journal.to_dict(return_level)

        return response


    def __repr__(self):
        return "<Work ( {} ) {} '{}...'>".format(self.paper_id, self.doi, self.paper_title[0:20])



