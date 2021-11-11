from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
from collections import defaultdict
from time import sleep

from app import db


# alter table recordthresher_record add column started_label text;
# alter table recordthresher_record add column started datetime;
# alter table recordthresher_record add column finished datetime;
# alter table recordthresher_record add column work_id bigint



class Record(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "recordthresher_record"

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

    # queues
    started = db.Column(db.DateTime)
    finished = db.Column(db.DateTime)
    started_label = db.Column(db.Text)

    # relationship to works is set in Work
    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"))

    def get_or_mint_work(self):
        pass


    def process(self):
        from models import Work

        self.insert_dict = {}
        print("processing record! {}".format(self.id))
        # self.work = self.get_or_mint_work()
        # self.work.refresh()
        q = "select paper_id from mid.work where match_title = f_matching_string(:title) and (len(f_normalize_title(:title)) > 3) limit 20;"
        print(f"this record: {self.record_webpage_url} {self.title}")
        rows = db.session.execute(text(q), {"title": self.title}).fetchall()
        paper_ids = [row[0] for row in rows]
        print(f"work paper_ids that match title: {paper_ids}")
        if paper_ids:
            matching_works = Work.query.options(orm.Load(Work).raiseload('*')).filter(Work.paper_id.in_(paper_ids)).all()
            urls = ["https://openalex-guts.herokuapp.com/work/id/{}".format(w.paper_id) for w in matching_works]
            print(f"works: {urls}")
            print("...... ")
            # sleep(10)


    def to_dict(self, return_level="full"):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Record ( {} ) doi:{}, pmh:{}, pmid:{}>".format(self.id, self.doi, self.pmh_id, self.pmid)


