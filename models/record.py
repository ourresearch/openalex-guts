from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
from collections import defaultdict
from time import sleep
import datetime

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


    def get_insert_fieldnames(self, table_name=None):
        lookup = {
            "mid.record_match": ["record_id", "updated", "matching_work_id", "added"]
        }
        if table_name:
            return lookup[table_name]
        return lookup


    def get_or_mint_work(self):
        from models.work import Work

        paper_ids = []
        print(f"trying to match this record: {self.record_webpage_url} {self.doi} {self.title}")
        if self.doi:
            q = "select paper_id from mid.work where doi_lower=:doi;"
            rows = db.session.execute(text(q), {"doi": self.doi}).fetchall()
            paper_ids = [row[0] for row in rows]
            print(f"work paper_ids that match doi: {paper_ids}")

        if not paper_ids:
            pass
            # try to match by pmid

        if not paper_ids:
            q = "select paper_id from mid.work where match_title = f_matching_string(:title) and (len(match_title) > 3) limit 20;"
            rows = db.session.execute(text(q), {"title": self.title}).fetchall()
            paper_ids = [row[0] for row in rows]
            print(f"work paper_ids that match title: {paper_ids}")

        if paper_ids:
            matching_work = Work.query.options(orm.Load(Work).raiseload('*')).filter(Work.paper_id.in_(paper_ids)).order_by(Work.citation_count.desc()).first()
            matching_work_id = matching_work.id
            url = f"https://openalex-guts.herokuapp.com/work/id/{matching_work_id}"
            print(f"found a match for this work: {url}")
            # sleep(10)
        else:
            print("no match")
            # mint a work
            matching_work = None
            matching_work_id = "null"

        self.insert_dict = {"mid.record_match": "('{record_id}', '{updated}', {matching_work_id}, '{added}')".format(
                              record_id=self.id,
                              updated=self.updated,
                              matching_work_id=matching_work_id,
                              added=datetime.datetime.utcnow().isoformat()
                            )}

        return matching_work



    def process(self):
        from models import Work

        self.insert_dict = {}
        print("processing record! {}".format(self.id))

        self.work = self.get_or_mint_work()
        # self.work.refresh()




    def to_dict(self, return_level="full"):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        return "<Record ( {} ) doi:{}, pmh:{}, pmid:{}>".format(self.id, self.doi, self.pmh_id, self.pmid)


