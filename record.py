import datetime
import json
import re
from copy import deepcopy


from app import db

# alter table recordthresher_record add column started_label text
# alter table recordthresher_record add column started datetime
# alter table recordthresher_record add column finished datetime

class Record(db.Model):
    __tablename__ = "recordthresher_record"

    id = db.Column(db.Text, primary_key=True)
    updated = db.Column(db.DateTime)

    record_type = db.Column(db.Text)
    doi = db.Column(db.Text)
    pmid = db.Column(db.Text)
    pmh_id = db.Column(db.Text)

    title = db.Column(db.Text)
    published_date = db.Column(db.DateTime)
    genre = db.Column(db.Text)

    abstract = db.Column(db.Text)
    mesh = db.Column(db.Text)

    citations = db.Column(db.Text)
    authors = db.Column(db.Text)
    mesh = db.Column(db.Text)

    repository_id = db.Column(db.Text)
    journal_id = db.Column(db.Text)
    journal_issn_l = db.Column(db.Text)

    record_webpage_url = db.Column(db.Text)
    record_webpage_archive_url = db.Column(db.Text)
    record_structured_url = db.Column(db.Text)
    record_structured_archive_url = db.Column(db.Text)

    work_pdf_url = db.Column(db.Text)
    work_pdf_archive_url = db.Column(db.Text)
    is_work_pdf_url_free_to_read = db.Column(db.Boolean)

    is_oa = db.Column(db.Boolean)
    oa_date = db.Column(db.DateTime)
    open_license = db.Column(db.Text)
    open_version = db.Column(db.Text)

    started = db.Column(db.DateTime)
    finished = db.Column(db.DateTime)

    def update(self):
        print("updating! {}".format(self.id))

    def __repr__(self):
        return "<Record ( {} ) {}, {}, {}>".format(self.id, self.record_type, self.doi, self.title)
