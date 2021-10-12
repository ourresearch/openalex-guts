import datetime

import shortuuid
import random

from app import db
from util import normalize_title


class Work(db.Model):

    __tablename__ = "recordthresher_work"

    id = db.Column(db.BigInteger, primary_key=True)
    created = db.Column(db.DateTime)
    updated = db.Column(db.DateTime)
    title = db.Column(db.Text)
    normalized_title = db.Column(db.Text)


    id = db.Column(db.Text, primary_key=True)
    updated = db.Column(db.DateTime)

    # ids
    record_type = db.Column(db.Text)
    doi = db.Column(db.Text)
    pmid = db.Column(db.Text)
    pmh_id = db.Column(db.Text)

    # metadata
    title = db.Column(db.Text)
    published_date = db.Column(db.DateTime)
    genre = db.Column(db.Text)
    abstract = db.Column(db.Text)
    mesh = db.Column(db.Text)

    # related tables
    citations = db.Column(db.Text)
    authors = db.Column(db.Text)
    mesh = db.Column(db.Text)

    # venue links
    repository_id = db.Column(db.Text)
    journal_id = db.Column(db.Text)
    journal_issn_l = db.Column(db.Text)

    # record data
    record_webpage_url = db.Column(db.Text)
    record_webpage_archive_url = db.Column(db.Text)
    record_structured_url = db.Column(db.Text)
    record_structured_archive_url = db.Column(db.Text)

    # oa and urls
    work_pdf_url = db.Column(db.Text)
    work_pdf_archive_url = db.Column(db.Text)
    is_work_pdf_url_free_to_read = db.Column(db.Boolean)
    is_oa = db.Column(db.Boolean)
    oa_date = db.Column(db.DateTime)
    open_license = db.Column(db.Text)
    open_version = db.Column(db.Text)

    # for processing
    normalized_title = db.Column(db.Text)

    # relationships
    records = db.relationship(
        "Record",
        lazy='subquery',
        backref="work"
    )

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
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

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
