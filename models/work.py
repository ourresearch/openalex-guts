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
    citation_count = db.Column(db.BigInteger)
    created_date = db.Column(db.DateTime)
    doi_lower = db.Column(db.Text)

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
        JSON_ELASTIC_VERSION_STRING = "includes some orcid"
        # print("processing work! {}".format(self.id))
        # self.json_full = jsonify_fast_no_sort_raw(self.to_dict())
        self.json_elastic = jsonify_fast_no_sort_raw(self.to_dict(return_level="elastic"))
        # has to match order of get_insert_fieldnames
        json_elastic_escaped = self.json_elastic.replace("'", "''").replace("%", "%%").replace(":", "\:")
        if len(json_elastic_escaped) > 65000:
            print("Error: json_elastic_escaped too long for paper_id {}, skipping".format(self.work_id))
            json_elastic_escaped = None
        self.insert_dict = {"mid.work_json": "({paper_id}, '{updated}', '{json_elastic}', '{version}')".format(
                                                                  paper_id=self.paper_id,
                                                                  updated=datetime.datetime.utcnow().isoformat(),
                                                                  json_elastic=json_elastic_escaped,
                                                                  version=JSON_ELASTIC_VERSION_STRING
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
        keys = ["work_id", "work_title", "year", "publication_date", "doc_type", "volume", "issue", "first_page", "last_page", "citation_count"]
        response = {key: getattr(self, key) for key in keys}
        response["ids"] = {}
        if self.doi:
            response["ids"]["doi"] = [self.doi_lower, self.doi_url]
        if self.extra_ids:
            response["ids"].update({extra_id.id_type: extra_id.to_dict(return_level) for extra_id in self.extra_ids})

        if self.journal:
            response["journal"] = self.journal.to_dict(return_level)
        if self.unpaywall:
            response["unpaywall"] = self.unpaywall.to_dict(return_level)
        response["affiliations"] = [affiliation.to_dict(return_level) for affiliation in self.affiliations_sorted]
        response["concepts"] = [concept.to_dict(return_level) for concept in self.concepts_sorted]
        response["locations"] = [location.to_dict(return_level) for location in self.locations_sorted]

        if return_level == "full":
            response["records"] = [record.to_dict(return_level) for record in self.records]
            response["locations"] = [location.to_dict(return_level) for location in self.locations_sorted]
            response["mesh"] = [mesh.to_dict(return_level) for mesh in self.mesh]
            response["citations"] = [citation.to_dict(return_level) for citation in self.citations]
            if self.abstract:
                response["abstract"] = self.abstract.to_dict(return_level)
            else:
                response["abstract"] = None

        return response


    def __repr__(self):
        return "<Work ( {} ) {} '{}...'>".format(self.paper_id, self.doi, self.paper_title[0:20])



