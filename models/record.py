from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import deferred
from sqlalchemy.orm import foreign, remote
from collections import defaultdict
from time import sleep
import datetime
import json

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
    publisher = db.Column(db.Text)
    institution_host = db.Column(db.Text)
    is_retracted = db.Column(db.Boolean)
    volume = db.Column(db.Text)
    issue = db.Column(db.Text)
    first_page = db.Column(db.Text)
    last_page = db.Column(db.Text)

    # related tables
    citations = db.Column(db.Text)
    authors = db.Column(db.Text)
    mesh = db.Column(db.Text)

    # venue links
    repository_id = db.Column(db.Text)
    journal_id = db.Column(db.Text)
    journal_issn_l = db.Column(db.Text, db.ForeignKey("mid.journal.issn"))
    venue_name = db.Column(db.Text)

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

    # set by Xplenty
    match_title = db.Column(db.Text)
    normalized_type = db.Column(db.Text)
    normalized_doc_type = db.Column(db.Text)

    # relationship to works is set in Work
    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"))

    # work_matches_by_title = db.relationship(
    #     'Work',
    #     lazy='subquery',
    #     viewonly=True,
    #     # foreign_keys="Work.match_title",
    #     primaryjoin="and_(func.length(foreign(Record.match_title)) > 20, foreign(Record.match_title) == remote(Work.match_title))"
    # )
    #
    # work_matches_by_doi = db.relationship(
    #     'Work',
    #     lazy='subquery',
    #     viewonly=True,
    #     # foreign_keys="Work.doi_lower",
    #     primaryjoin="and_(foreign(Record.doi) != None, foreign(Record.doi) == remote(Work.doi_lower))"
    # )

    @property
    def score(self):
        if self.record_type == "crossref_doi":
            return 100
        if self.record_type == "pubmed_record":
            return 50
        return 10  # pmh_record

    # necessary right now because multiple journals can match on issn_ls in the mid.journal table alas. once that is fixed can normalize this.
    @cached_property
    def journal(self):
        if not self.journals:
            return None
        return self.journals[0]

    @cached_property
    def siblings(self):
        from models import Work
        response = [self.to_dict()]
        work_id = self.work_id #previously linked
        work_obj = self.get_or_mint_work()
        if work_obj:
            work_to_dict = {"RECORD_TYPE": "WORK"}
            work_to_dict.update(work_obj.to_dict())
            work_to_dict["work_id"] = work_to_dict["id"]
            del work_to_dict["id"]
            work_to_dict["score"] = 100
            work_to_dict["abstract_inverted_index"] = work_to_dict["abstract_inverted_index"] != None
            response += [work_to_dict]
            work_id = work_obj.id
        if work_id:
            work_linked_records = Record.query.filter(Record.work_id==work_obj.id).all()
            response += [record.to_dict() for record in work_linked_records]
        response = sorted(response, key=lambda x: x["score"], reverse=True)
        return response


    def get_or_mint_work(self, new_work_id_if_needed):
        from models.work import Work

        matching_works = []
        matching_work = None
        matching_work_id = None
        print(f"trying to match this record: {self.record_webpage_url} {self.doi} {self.title}")

        if False:
            # by doi
            if self.work_matches_by_doi:
                print("found by doi")
                matching_works = self.work_matches_by_doi

            # by pmid
            # later

            # by title
            if not matching_works and self.genre != "component":
                if self.work_matches_by_title:
                    print("found by title")
                    matching_works = self.work_matches_by_title

            # by url
            if not matching_works:
                q = """
                select paper_id
                from mid.location t1
                where (lower(replace(t1.source_url, 'https', 'http')) = lower(replace(:url1, 'https', 'http'))
                or (lower(replace(t1.source_url, 'https', 'http')) = lower(replace(:url2, 'https', 'http'))))
                """
                matching_works = db.session.execute(text(q), {"url1": self.record_webpage_url, "url2": self.work_pdf_url}).first()
                if matching_works:
                    print(f"found by url")
                    matching_work_id = matching_works[0]

            # by pmhid
            if not matching_works:
                q = """
                select paper_id
                from mid.location t1
                where pmh_id=:pmh_id
                """
                matching_works = db.session.execute(text(q), {"pmh_id": self.pmh_id}).first()
                if matching_works:
                    print(f"found by pmhid")
                    matching_work_id = matching_works[0]

        if matching_works:
            if not matching_work_id:
                sorted_matching_works = sorted(matching_works, key=lambda x: x.citation_count, reverse=True)
                matching_work = sorted_matching_works[0]
                matching_work_id = matching_work.id
            url = f"https://api.openalex.org/W{matching_work_id}"
            print(f"*********found a match for this work: {url}")
            print(f"don't do anything else with this")
            # don't do anything else
            # sleep(10)
        else:
            print("_____________no match")
            # mint a work

            self.mint_work(new_work_id_if_needed)
            matching_work_id = "null"

        # self.insert_dict = [{"mid.record_match": "('{record_id}', '{updated}', {matching_work_id}, '{added}')".format(
        #                       record_id=self.id,
        #                       updated=self.updated,
        #                       matching_work_id=matching_work_id,
        #                       added=datetime.datetime.utcnow().isoformat()
        #                     )}]

        return matching_work


    def mint_work(self, new_work_id):
        from models import Work
        from models import Venue

        journal_id = self.journal.journal_id if self.journal else None

        # assumes already tried a match
        self.work_id = new_work_id

        new_work = Work()
        new_work.paper_id = new_work_id
        new_work.doi = self.doi
        new_work.doi_lower = self.doi  # already lowered from recordthresher
        new_work.original_title = self.title
        new_work.match_title = self.match_title
        new_work.journal_id = journal_id
        new_work.genre = self.normalized_type
        new_work.doc_type = self.normalized_doc_type
        db.session.add(new_work)

        print(f"MADE A NEW WORK!!! {new_work}")


    def process_record(self, new_work_id_if_needed):
        from models import Work

        self.insert_dict = [{}]
        print("processing record! {}".format(self.id))

        self.work = self.get_or_mint_work(new_work_id_if_needed)
        # self.work.mint()


    def to_dict(self, return_level="full"):
        print("here")
        response = {
            "RECORD_TYPE": "RECORD",
            "id": self.id,
            "api_url": f"http://localhost:5007/records/{self.id}",
            "published_date": self.published_date,
            "updated": self.updated,
            "record_type": self.record_type,
            "doi": self.doi,
            "pmid": self.pmid,
            "pmh_id": self.pmh_id,
            "title": self.title,
            "genre": self.genre,
            "abstract": self.abstract[:100] if self.abstract else None,
            "mesh": json.loads(self.mesh) if self.mesh else None,
            "citations": json.loads(self.citations) if self.citations else None,
            "authors": json.loads(self.authors) if self.authors else None,
            "repository_id": self.repository_id,
            "journal_id": self.journal_id,
            "journal_issn_l": self.journal_issn_l,
            "record_webpage_url": self.record_webpage_url,
            # "record_webpage_archive_url": self.record_webpage_archive_url,
            "record_structured_url": self.record_structured_url,
            # "record_structured_archive_url": self.record_structured_archive_url,
            "work_pdf_url": self.work_pdf_url,
            # "work_pdf_archive_url": self.work_pdf_archive_url,
            "is_work_pdf_url_free_to_read": self.is_work_pdf_url_free_to_read,
            "is_oa": self.is_oa,
            "oa_date": self.oa_date,
            "open_license": self.open_license,
            "open_version": self.open_version,
            "publisher": self.publisher,
            "institution_host": self.institution_host,
            "is_retracted": self.is_retracted,
            }
        response["score"] = self.score
        return response

    def __repr__(self):
        return "<Record ( {} ) doi:{}, pmh:{}, pmid:{}>".format(self.id, self.doi, self.pmh_id, self.pmid)


