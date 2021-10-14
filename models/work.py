import datetime

import shortuuid
import random

from app import db
from util import normalize_title


class Work(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_main_papers"

    # __table_args__ = {'schema': 'mid'}
    # __tablename__ = "work"

    paper_id = db.Column(db.BigInteger, primary_key=True)
    # created = db.Column(db.DateTime)
    # updated = db.Column(db.DateTime)
    doi = db.Column(db.Text)
    doc_type = db.Column(db.Text)
    paper_title = db.Column(db.Text)
    original_title = db.Column(db.Text)
    # book_title character varying(65535),
    year = db.Column(db.Numeric)
    publication_date = db.Column(db.DateTime)
    online_date = db.Column(db.DateTime)
    publisher = db.Column(db.Text)
    journal_id = db.Column(db.BigInteger)
    # conference_series_id bigint,
    # conference_instance_id bigint,
    # volume character varying(65535),
    # issue character varying(65535),
    # first_page character varying(65535),
    # last_page character varying(65535),
    # reference_count bigint,
    # citation_count bigint,
    # estimated_citation bigint,
    # original_venue character varying(65535),
    # family_id bigint,
    # family_rank bigint,
    created_date = db.Column(db.DateTime)
    doi_lower = db.Column(db.Text)


    def __init__(self, **kwargs):
        self.id = random.randint(1000, 10000000)
        self.created = datetime.datetime.utcnow().isoformat()
        self.updated = self.created
        super(Work, self).__init__(**kwargs)

    def refresh(self):
        print("refreshing! {}".format(self.id))
        self.title = self.records[0].title
        self.normalized_title = calc_normalized_title(self.title)

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

    def to_dict(self):
        response = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        response["records"] = [record.to_dict() for record in self.records]

        if self.abstract:
            response["abstract"] = self.abstract.to_dict()
        else:
            response["abstract"] = None

        if self.journal:
            response["journal"] = self.journal.to_dict()
        else:
            response["journal"] = None
        response["mesh"] = [mesh.to_dict() for mesh in self.mesh]
        response["citations"] = [citation.to_dict() for citation in self.citations]
        response["affiliations"] = [affiliation.to_dict() for affiliation in self.affiliations]
        response["concepts"] = [concept.to_dict() for concept in self.concepts]
        response["locations"] = [location.to_dict() for location in self.locations]
        return response

    def __repr__(self):
        return "<Work ( {} ) {}>".format(self.id, self.title)


def calc_normalized_title(title, repository_id=None):
    if not title:
        return None

    if repository_id:
        pass
        # eventually make it handle these things, or the repository_id
        # if self.endpoint_id == '63d70f0f03831f36129':
        #     # figshare. the record is for a figure but the title is from its parent article.
        #     return None
        # # repo specific rules
        # # AMNH adds biblio to the end of titles, which ruins match.  remove this.
        # # example http://digitallibrary.amnh.org/handle/2246/6816 oai:digitallibrary.amnh.org:2246/6816
        # if "amnh.org" in self.id:
        #     # cut off the last part, after an openning paren
        #     working_title = re.sub("(Bulletin of.+no.+\d+)", "", working_title, re.IGNORECASE | re.MULTILINE)
        #     working_title = re.sub("(American Museum nov.+no.+\d+)", "", working_title, re.IGNORECASE | re.MULTILINE)
        # # for endpoint 0dde28a908329849966, adds this to end of all titles, so remove (eg http://hdl.handle.net/11858/00-203Z-0000-002E-72BD-3)
        # working_title = re.sub("vollständige digitalisierte Ausgabe", "", working_title, re.IGNORECASE | re.MULTILINE)

    return normalize_title(title)
