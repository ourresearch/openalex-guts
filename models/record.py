import datetime
import json

from cached_property import cached_property
from sqlalchemy import orm, and_, desc
from sqlalchemy.orm import raiseload
from sqlalchemy.sql.expression import func

from app import db
from const import MAX_AFFILIATIONS_PER_AUTHOR, MIN_CHARACTERS_PER_AFFILIATION, \
    BAD_TITLES
from models.location import normalize_license
from models.merge_utils import merge_primary_with_parsed
from util import normalize_title_like_sql


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
    normalized_book_publisher = db.Column(db.Text)
    normalized_conference = db.Column(db.Text)
    institution_host = db.Column(db.Text)
    is_retracted = db.Column(db.Boolean)
    volume = db.Column(db.Text)
    issue = db.Column(db.Text)
    first_page = db.Column(db.Text)
    last_page = db.Column(db.Text)

    # related tables
    citations = db.Column(db.Text)
    authors = db.Column(db.Text)

    # source links
    repository_id = db.Column(db.Text)
    # the journal_id in record is not the openalex journal ID
    journal_issns = db.Column(db.Text)
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
    funders = db.Column(db.Text)

    # relationship to works is set in Work
    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"))

    @property
    def has_affiliations(self):
        return self.cleaned_affiliations_count > 0

    @property
    def cleaned_affiliations_count(self):
        count = 0
        for author in self.cleaned_authors_json:
            count += len(author.get("affiliation", []))
        return count

    @property
    def affiliations_count(self):
        count = 0
        for author in self.authors_json:
            count += len(author.get("affiliation", []))
        return count

    @property
    def has_citations(self):
        return bool(self.citations_json)

    @property
    def cleaned_authors_json(self):
        j = json.loads(self.authors or '[]')
        for author in j:
            if 'affiliations' in author and 'affiliation' not in author:
                author['affiliation'] = author['affiliations']
                del author['affiliations']
            final_affs = []
            if len(author.get("affiliation", [])) > MAX_AFFILIATIONS_PER_AUTHOR:
                author['affiliation'] = final_affs
                continue
            for aff in author.get('affiliation', []):
                if (isinstance(aff, dict) and len(aff.get('name', '') or '') >= MIN_CHARACTERS_PER_AFFILIATION) or (isinstance(aff, str) and len(aff) >= MIN_CHARACTERS_PER_AFFILIATION):
                    final_affs.append(aff)
            author['affiliation'] = final_affs
        return j

    @property
    def authors_json(self):
        return json.loads(self.authors or '[]')

    @property
    def affiliations_per_author(self):
        return self.cleaned_affiliations_count / len(
            self.authors_json)

    @property
    def clean_affiliations_per_author(self):
        return self.cleaned_affiliations_count / len(self.cleaned_authors_json)

    @property
    def affiliations_probably_invalid(self):
        return self.affiliations_per_author > MAX_AFFILIATIONS_PER_AUTHOR

    @property
    def citations_json(self):
        return json.loads(self.citations or '[]')

    @property
    def is_hal_record(self):
        return self.pmh_id and 'oai:hal' in self.pmh_id.lower()

    @property
    def best_hal_affiliations_record(self):
        if not self.hal_records:
            return None
        best_count, best_record = 0, self.hal_records[0]
        for record in self.hal_records:
            if record.cleaned_affiliations_count > best_count:
                best_count = record.cleaned_affiliations_count
                best_record = record
        return best_record

    @cached_property
    def oa_status_manual(self):
        result = db.session.execute(
            '''
            SELECT oa_status 
            FROM ins.oa_status_manual 
            WHERE recordthresher_id = :recordthresher_id
            ''',
            {'recordthresher_id': self.id}
        ).first()

        return result['oa_status'] if result else None

    @cached_property
    def with_parsed_data(self):
        parsed_records = {'parseland_record': self.parseland_record,
                          'pdf_record': self.pdf_record,
                          'hal_record': self.best_hal_affiliations_record,
                          'mag_record': self.mag_record,
                          'legacy_record': self.legacy_records[0] if self.legacy_records else None}
        return merge_primary_with_parsed(self, **parsed_records)

    def __init__(self, **kwargs):
        super(Record, self).__init__(**kwargs)

    @property
    def display_open_license(self):
        return normalize_license(self.open_license)

    @property
    def display_open_license_id(self):
        open_license = normalize_license(self.open_license)
        return f"https://openalex.org/licenses/{open_license}" if open_license else None

    @property
    def score(self):
        if self.record_type == "override":
            return 500
        if self.record_type == "crossref_doi":
            return 100
        if self.record_type == "pubmed_record":
            return 50
        return 10  # pmh_record

    # necessary right now because multiple journals can match on issn_ls in the mid.journal table alas. once that is fixed can normalize this.
    @cached_property
    def journal(self):
        now = datetime.datetime.utcnow()
        if not self.journals:
            return None
        sorted_journals = sorted(self.journals, key=lambda
            x: x.full_updated_date if x.full_updated_date else now,
                                 reverse=True)
        best_journal = sorted_journals[0]
        return best_journal.merged_into_source or best_journal

    def get_or_mint_work(self):
        from models.work import Work
        if self.record_type == "override":
            # This should already be connected to a work via work_id
            return
        if not self.is_primary_record():
            print(
                f'record {self.id} is of type {self.record_type} '
                'and is matched to a primary record via an association table'
            )
            self.work_id = -1
            return
        now = datetime.datetime.utcnow()

        if self.genre == "component":
            self.work_id = -1
            print(f"{self.id} is a component, so skipping")
            return

        matching_work = None
        print(
            f"trying to match this record: {self.record_webpage_url} {self.doi} {self.title}")

        # by doi
        if matching_works := [w for w in self.work_matches_by_doi if
                              not w.merge_into_id]:
            print("found by doi")
            sorted_matching_works = sorted(matching_works, key=lambda
                x: x.full_updated_date if x.full_updated_date else now,
                                           reverse=True)
            matching_work = sorted_matching_works[0]

        # by pmid
        if not matching_works:
            if matching_works := [w for w in self.work_matches_by_pmid if
                                  not w.merge_into_id]:
                print("found by pmid")
                sorted_matching_works = sorted(matching_works, key=lambda
                    x: x.full_updated_date if x.full_updated_date else now,
                                               reverse=True)
                matching_work = sorted_matching_works[0]

        # by arxiv_id
        if not matching_works:
            if matching_works := [w for w in self.work_matches_by_arxiv_id if
                                  not w.merge_into_id]:
                print("found by arxiv_id")
                sorted_matching_works = sorted(matching_works, key=lambda
                    x: x.full_updated_date if x.full_updated_date else now,
                                               reverse=True)
                matching_work = sorted_matching_works[0]

        # arxiv preprint from datacite, with associated doi
        if not matching_works and self.record_type == "datacite_doi" and self.arxiv_id:
            related_version = db.session.query(RecordRelatedVersion).filter(
                RecordRelatedVersion.doi == self.doi,
            ).first()
            if related_version:
                print(
                    f"trying to match by related_version_doi {related_version.related_version_doi}")

                match = db.session.query(Work).options(raiseload('*')).filter(
                    Work.doi_lower == related_version.related_version_doi,
                    Work.merge_into_id.is_(None)
                ).first()

                if match:
                    matching_work = match
                    print(
                        f"found by related_version_doi {related_version.related_version_doi}")

        # by title
        if not matching_work:
            # don't use self.work_matches_by_title because sometimes there are many matches and
            # setting lazy='dynamic' to enable a limit here causes all properties of works to be loaded
            work_matches_by_title = db.session.query(Work).options(
                orm.Load(Work).joinedload(Work.affiliations).raiseload('*'),
                orm.Load(Work).raiseload('*')
            ).filter(
                and_(
                    Work.original_title.not_in(BAD_TITLES),
                    self.normalized_title is not None,
                    len(self.normalized_title) > 19,
                    Work.unpaywall_normalize_title == self.normalized_title
                )
            ).order_by(
                desc(Work.full_updated_date)
            ).limit(50).all()

            if matching_works := [w for w in work_matches_by_title if
                                  not w.merge_into_id]:
                sorted_matching_works = sorted(matching_works, key=lambda
                    x: x.full_updated_date if x.full_updated_date else now,
                                               reverse=True)

                # just look at the first 20 matches
                for matching_work_temp in sorted_matching_works[:20]:
                    if matching_work_temp.doi_lower and self.doi and matching_work_temp.doi_lower != self.doi:
                        print(
                            f"titles match but dois don't so don't merge this for now")
                        continue

                    if not self.with_parsed_data or not self.with_parsed_data.authors or not self.with_parsed_data.authors_json:
                        # is considered a match
                        matching_work = matching_work_temp
                        print(
                            f"no authors for {self.id}, so considering it an author match")
                        break

                    if matching_work_temp.matches_authors_in_record(
                            self.with_parsed_data.authors):
                        matching_work = matching_work_temp
                        print(f"MATCHING AUTHORS for {self.id}!")
                        break
                    else:
                        print(f"authors don't match for {self.id}!")

                if matching_work:
                    print(
                        f"found by title {self.normalized_title} on {matching_work.id} to {self.id}")

        if (not matching_work) \
                and (not self.doi) and (not self.pmid) and (
                not self.pmcid) and (not self.arxiv_id) \
                and ((not self.title) or (len(self.normalized_title) < 20)):
            self.work_id = -1
            print(
                f"{self.id} does not have a strong identifier and has no title, or title is too short, skipping")
            return

        if matching_work:
            print(
                f"FOUND A MATCH: https://openalex-guts.herokuapp.com/W{matching_work.paper_id}")
            self.work_id = matching_work.paper_id  # link the record to the work
            matching_work.full_updated_date = None  # prep the work for needing an update
        else:
            print("no match, so minting")
            self.mint_work()
        return

    def is_primary_record(self):
        return self.record_type and self.record_type in {
            "crossref_doi",
            "datacite_doi",
            "pubmed_record",
            "pmh_record",
            "mag_location",
            "override",
        }

    def mint_work(self):
        from models import Work
        from util import normalize_doi

        journal_id = self.journal.journal_id if self.journal else None

        new_work = Work()
        new_work.created_date = datetime.datetime.utcnow().isoformat()
        # new_work.paper_id = new_work_id
        new_work.doi = normalize_doi(self.doi, return_none_if_error=True)
        new_work.doi_lower = normalize_doi(self.doi, return_none_if_error=True)
        new_work.original_title = self.title[:60000] if self.title else None
        new_work.unpaywall_normalize_title = self.normalized_title if self.normalized_title else normalize_title_like_sql(
            self.title)
        new_work.journal_id = journal_id
        new_work.genre = self.normalized_work_type
        new_work.doc_type = self.normalized_doc_type
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
        return "<Record ( {} ) doi:{}, pmh:{}, pmid:{}>".format(self.id,
                                                                self.doi,
                                                                self.pmh_id,
                                                                self.pmid)


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
    edited-book,edited-book,Book
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
    article,journal-article,Journac
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
        work_type_lookup[lookup.strip()] = {
            "work_type": work_type if work_type else None,
            "doc_type": doc_type if doc_type else None}


class RecordFulltext(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "record_fulltext"

    recordthresher_id = db.Column(db.Text,
                                  db.ForeignKey("ins.recordthresher_record.id"),
                                  primary_key=True)
    _fulltext = db.Column('fulltext', db.Text)
    truncated_fulltext = db.column_property(
        func.substring(_fulltext, 1, 200000))


class RecordthresherParentRecord(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "recordthresher_parent_record"

    record_id = db.Column(db.Text, primary_key=True)
    parent_record_id = db.Column(db.Text, primary_key=True)


class RecordRelatedVersion(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = 'record_related_version'

    doi = db.Column(db.Text, db.ForeignKey("ins.recordthresher_record.doi"),
                    primary_key=True)
    related_version_doi = db.Column(db.Text, primary_key=True)
    type = db.Column(db.Text, primary_key=True)


Record.fulltext = db.relationship(RecordFulltext, lazy='selectin',
                                  viewonly=True, uselist=False)
