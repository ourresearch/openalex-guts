import re
from util import clean_doi, entity_md5, normalize_title_like_sql, \
    matching_author_strings, get_crossref_json_from_unpaywall, \
    words_within_distance
from const import PREPRINT_JOURNAL_IDS, REVIEW_JOURNAL_IDS, \
    MAX_AFFILIATIONS_PER_AUTHOR
from app import db
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models import Work

def get_libguides_ids():
    ids = db.session.execute('SELECT * FROM libguides_paper_ids;').fetchall()
    return set([_id[0] for _id in ids])


LIBGUIDES_IDS = get_libguides_ids()

class WorkTypeDetective:
    def __init__(self, work: 'Work') -> None:
        self.work = work
    
    @property
    def type_crossref_calculated(self):
        # legacy type used < 2023-08
        # (but don't get rid of it, it's used to derive the new type (display_genre))
        if self.looks_like_paratext:
            return "other"
        if self.work.genre:
            return self.work.genre
        if self.work.doc_type:
            lookup_mag_to_crossref_type = {
                "Journal": "journal-article",
                "Thesis": "dissertation",
                "Conference": "proceedings-article",
                "Repository": "posted-content",
                "Book": "book",
                "BookChapter": "book-chapter",
                "Dataset": "dataset",
            }
            if mag_type := lookup_mag_to_crossref_type.get(self.work.doc_type):
                return mag_type
        if self.work.journal and self.work.journal.type and 'book' in self.work.journal.type:
            return 'book-chapter'
        return 'journal-article'

    def get_record(self, record_type):
        for record in self.work.records_sorted:
            if record.record_type == record_type:
                return record

    @property
    def is_preprint(self):
        if r := self.get_record('crossref_doi'):
            crossref_json = get_crossref_json_from_unpaywall(r.doi)
            if crossref_json and crossref_json.get('subtype', '') == 'preprint':
                return True
        return self.work.journal_id in PREPRINT_JOURNAL_IDS or (self.work.journal_id is None and self.work.genre == 'posted-content') # From Unpaywall

    @property
    def is_review(self):
        return self.work.journal_id in REVIEW_JOURNAL_IDS or (
                self.work.original_title and words_within_distance(self.work.original_title.lower(), 'a', 'review', 2))

    @property
    def type_calculated(self):
        # this is what goes into the `Work.type` attribute
        if self.looks_like_paratext:
            return "paratext"
        if self.work.original_title and 'supplementary table' in self.work.original_title.lower():
            return 'supplementary-materials'
        if self.is_review:
            return 'review'
        if self.is_preprint:
            return 'preprint'
        if self.work.paper_id in LIBGUIDES_IDS:
            return 'libguides'

        # infer "erratum", "editorial", "letter" types:
        try:
            if self.guess_type_from_title:
                # todo: do another pass at this. improve precision and recall.
                return self.guess_type_from_title
        except AttributeError:
            pass

        if r := self.get_record('pubmed_record'):
            # pubmed is generally better than crossref when it comes to work type
            lookup_pubmed_to_openalex_type = {
                'Journal Article': 'article',
                'Review': 'review',
                'Letter': 'letter',
                'Comment': 'letter',  # TODO: revisit this
                'Editorial': 'editorial',
                'Systematic Review': 'review',
                'Meta-Analysis': 'review',
                'Published Erratum': 'erratum',
                'Retraction of Publication': 'retraction',
                'Preprint': 'preprint',
            }
            if r.genre in lookup_pubmed_to_openalex_type:
                return lookup_pubmed_to_openalex_type[r.genre]

        lookup_crossref_to_openalex_type = {
            "journal-article": "article",
            "proceedings-article": "article",
            "posted-content": "article",
            "book-part": "book-chapter",
            "journal-issue": "paratext",
            "journal": "paratext",
            "journal-volume": "paratext",
            "report-series": "paratext",
            "proceedings": "paratext",
            "proceedings-series": "paratext",
            "book-series": "paratext",
            "component": "paratext",
            "monograph": "book",
            "reference-book": "book",
            "book-set": "book",
            "edited-book": "book",
        }
        # return mapping from lookup if it's in there, otherwise pass-through
        return lookup_crossref_to_openalex_type.get(self.type_crossref_calculated,
                                                    self.type_crossref_calculated)

    @property
    def looks_like_paratext(self):
        if self.work.is_paratext:
            return True

        paratext_exprs = [
            r'^Author Guidelines$',
            r'^Author Index$'
            r'^Back Cover',
            r'^Back Matter',
            r'Back Matter]*$',
            r'Back Cover]*$',
            r'^Contents$',
            r'^Contents:',
            r'^Cover Image',
            r'^Cover Picture',
            r'^Editorial Board',
            r'Editor Report$',
            r'^Front Cover',
            r'\[Front Cover\]',
            r'\[Inside Back Cover.*\]',
            r'\[Back Inside Cover.*\]',
            r'^Frontispiece',
            r'^Graphical Contents List$',
            r'^Index$',
            r'^Inside Back Cover',
            r'^Inside Cover',
            r'Back Cover[\]\)]*$',
            r'^Inside Front Cover',
            r'^Issue Information',
            r'^List of contents',
            r'^List of Tables$',
            r'^List of Figures$',
            r'^List of Plates$',
            r'^Masthead',
            r'\[Masthead\]',
            r'^Pages de début$',
            r'^Title page',
            r"^Editor's Preface",
        ]

        for expr in paratext_exprs:
            if self.work.work_title and re.search(expr, self.work.work_title,
                                             re.IGNORECASE):
                return True

        return False

    @property
    def guess_type_from_title(self):
        erratum_exprs = [
            r'^erratum',
            r'erratum$',
            r'\[erratum',
            r'\(erratum',
        ]
        for expr in erratum_exprs:
            if self.work.work_title and re.search(expr, self.work.work_title,
                                             re.IGNORECASE):
                return "erratum"

        letter_exprs = [
            r'^(A )?letter:',
            r'^(A )?\[*letter to',
            r'^(A )?\[*letter from',
            r'^(A )?letter$',
            r'^(A )?\[*letter:',
            r'^(An )?Open letter'
        ]
        for expr in letter_exprs:
            if self.work.work_title and re.search(expr, self.work.work_title,
                                             re.IGNORECASE):
                return "letter"

        editorial_exprs = [
            r'^(An )?editorial:',
            r'^(An )?editorial$',
            r'^(An )?editorial comment',
            r'^(A )?guest editorial',
            r'^(An )?editorial note',
            r'^(An )?editorial -'
            r'(A )?editorial \w+:'
        ]
        for expr in editorial_exprs:
            if self.work.work_title and re.search(expr, self.work.work_title,
                                             re.IGNORECASE):
                return "editorial"

        return None
