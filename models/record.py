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
from util import normalize_title_like_sql


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
    pmcid = db.Column(db.Text)
    arxiv_id = db.Column(db.Text)

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
    # the journal_id in record is not the openalex journal ID
    journal_issn_l = db.Column(db.Text)
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

    normalized_title = db.Column(db.Text)

    # relationship to works is set in Work
    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"))

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
        sorted_journals = sorted(self.journals, key=lambda x: x.citation_count if x.citation_count else 0, reverse=True)
        return sorted_journals[0]


    def get_or_mint_work(self):
        from models.work import Work

        if self.genre == "component":
            self.work_id = -1
            print(f"{self.id} is a component, so skipping")
            return

        matching_work = None
        print(f"trying to match this record: {self.record_webpage_url} {self.doi} {self.title}")

        # by doi
        if self.work_matches_by_doi:
            print("found by doi")
            matching_works = self.work_matches_by_doi
            sorted_matching_works = sorted(matching_works, key=lambda x: x.citation_count if x.citation_count else 0, reverse=True)
            matching_work = sorted_matching_works[0]

        # by pmid or pmc_id or arxiv_id, later. match by id before match by title.

        # by title
        if not matching_work:
            if self.work_matches_by_title:
                matching_works = self.work_matches_by_title
                sorted_matching_works = sorted(matching_works, key=lambda x: x.citation_count if x.citation_count else 0, reverse=True)

                # just look at the first 20 matches
                for matching_work_temp in sorted_matching_works[:20]:
                    if matching_work_temp.doi_lower and self.doi and matching_work_temp.doi_lower != self.doi:
                        print(f"titles match but dois don't so don't merge this for now")
                        continue

                    if not self.authors or self.authors == []:
                        # is considered a match
                        matching_work = matching_work_temp
                        print(f"no authors for {self.id}, so considering it an author match")
                        break

                    if matching_work_temp.matches_authors_in_record(self.authors):
                        matching_work = matching_work_temp
                        print(f"MATCHING AUTHORS for {self.id}!")
                        break
                    else:
                        print(f"authors don't match for {self.id}!")

                if matching_work:
                    print(f"found by title {self.normalized_title} on {matching_work.id} to {self.id}")

        if (not matching_work) \
                and (not self.doi) and (not self.pmid) and (not self.pmcid) and (not self.arxiv_id) \
                and ((not self.title) or (len(self.title) < 20)):
            self.work_id = -1
            print(f"{self.id} does not have a strong identifier and has no title, or title is too short, skipping")
            return

        if matching_work:
            print(f"FOUND A MATCH: https://openalex-guts.herokuapp.com/W{matching_work.paper_id}")
            self.work_id = matching_work.paper_id   # link the record to the work
            matching_work.full_updated_date = None  # prep the work for needing an update
        else:
            print("no match, so minting")
            self.mint_work()
        return



    def mint_work(self):
        from models import Work
        from models import QueueWorks

        journal_id = self.journal.journal_id if self.journal else None

        new_work = Work()
        new_work.created_date = datetime.datetime.utcnow().isoformat()
        # new_work.paper_id = new_work_id
        new_work.doi = self.doi
        new_work.doi_lower = self.doi  # already lowered from recordthresher
        new_work.original_title = self.title[:60000] if self.title else None
        new_work.unpaywall_normalize_title = self.normalized_title if self.normalized_title else normalize_title_like_sql(self.title)
        new_work.journal_id = journal_id
        new_work.genre = self.normalized_work_type
        new_work.doc_type = self.normalized_doc_type
        new_work.queue = QueueWorks()
        db.session.add(new_work)
        self.work = new_work

        print(f"MADE A NEW WORK!!! {new_work} with recordthresher id {self.id}")

    @property
    def normalized_doc_type(self):
        if not self.genre:
            return None
        try:
            return work_type_lookup[self.genre.lower()]["doc_type"]
        except KeyError:
            return None

    @property
    def normalized_work_type(self):
        if not self.genre:
            return None
        try:
            return work_type_lookup[self.genre.lower()]["work_type"]
        except KeyError:
            return None

    def process_record(self):
        self.insert_dict = [{}]
        print("processing record! {}".format(self.id))

        self.get_or_mint_work()


    def __repr__(self):
        return "<Record ( {} ) doi:{}, pmh:{}, pmid:{}>".format(self.id, self.doi, self.pmh_id, self.pmid)



work_type_strings = """
    lookup_string,work_type,doc_type
    conference,proceedings,Conference
    proceedings-series,proceedings-series,
    journal-volume,journal-volume,
    book-series,book-series,
    dataset,dataset,Dataset
    info:eu-repo/semantics/report,report,
    guideline,other,
    preprint,posted-content,Repository
    report,report,
    thesis/dissertation,dissertation,Thesis
    corrected and republished article,journal-article,Journal
    observational study,journal-article,Journal
    systematic review,journal-article,Journal
    info:eu-repo/semantics/workingpaper,report,
    video-audio media,other,
    english abstract,other,
    personal narrative,other,
    info:eu-repo/semantics/other,other,
    letter,posted-content,Repository
    proceedings,proceedings,Conference
    technical report,report,
    peer-review,peer-review,
    program document,other,
    info:eu-repo/semantics/doctoralthesis,dissertation,Thesis
    info:eu-repo/semantics/masterthesis,dissertation,Thesis
    other,other,
    book,book,Book
    dissertation,dissertation,Thesis
    info:eu-repo/semantics/article,journal-article,Journal
    monograph,monograph,Book
    proceedings-article,proceedings-article,Conference
    data,dataset,Dataset
    info:eu-repo/semantics/conferencepaper,proceedings,Conference
    info:eu-repo/semantics/patent,other,
    info:eu-repo/semantics/preprint,posted-content,Repository
    journal,journal,
    practice guideline,other,
    book-set,book-set,
    grant,grant,
    congress,other,
    info:eu-repo/semantics/conferenceobject,proceedings-article,Conference
    journal article: accepted manuscript,journal-article,Journal
    report-series,report-series,
    news,posted-content,Repository
    reference-entry,reference-entry,
    book-part,book-part,
    clinical trial,journal-article,Journal
    editorial,journal-article,Journal
    info:eu-repo/semantics/book,book,Book
    journal article: publisher's accepted manuscript,journal-article,Journal
    posted-content,posted-content,Repository
    published erratum,other,
    reference-book,reference-book,
    retraction of publication,other,
    standard,standard,
    info:eu-repo/semantics/bookpart,book-chapter,BookChapter
    journal-issue,journal-issue,
    book-chapter,book-chapter,BookChapter
    interview,other,
    introductory journal article,journal-article,Journal
    historical article,journal-article,Journal
    journal article,journal-article,Journal
    journal-article,journal-article,Journal
    meta-analysis,journal-article,Journal
    article,journal-article,Journal
    case reports,journal-article,Journal
    component,component,
    info:eu-repo/semantics/lecture,other,
    journal article: published article,journal-article,Journal
    patient education handout,other,
"""

work_type_lines = work_type_strings.split("\n")
work_type_lookup = dict()
for line in work_type_lines:
    if line:
        (lookup, work_type, doc_type) = line.split(",")
        work_type_lookup[lookup.strip()] = {"work_type": work_type if work_type else None,
                                            "doc_type": doc_type if doc_type else None}

