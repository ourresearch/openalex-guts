import json

from app import db


def get_repository_institution_from_source_url(source_url):
    lookup = {"europepmc.org": "Europe PMC",
              "ncbi.nlm.nih.gov/pmc": "PMC",
              "pubmed.ncbi.nlm.nih.gov": "PubMed",
              "arxiv.org": "arXiv",
              "ci.nii.ac.jp": "CiNii",
              "ui.adsabs.harvard.edu": "SAO/NASA Astrophysics Data System",
              "dialnet.unirioja.es": "Dialnet",
              "pdfs.semanticscholar.org": "Semantic Scholar"}
    for key, value in lookup.items():
        if key in source_url:
            return value
    return None


def is_accepted(version):
    if version:
        if version == 'submittedVersion':
            return False
        elif version in ['acceptedVersion', 'publishedVersion']:
            return True
    return False


def is_published(version):
    if version:
        if version in ['submittedVersion', 'acceptedVersion']:
            return False
        elif version == 'publishedVersion':
            return True
    return False


class Location(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "location"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    source_url = db.Column(db.Text, primary_key=True)
    source_type = db.Column(db.Numeric)
    language_code = db.Column(db.Text)
    url = db.Column(db.Text)
    url_for_landing_page = db.Column(db.Text)
    url_for_pdf = db.Column(db.Text)
    host_type = db.Column(db.Text)
    version = db.Column(db.Text)
    license = db.Column(db.Text)
    repository_institution = db.Column(db.Text)
    pmh_id = db.Column(db.Text)
    is_best = db.Column(db.Boolean)
    oa_date = db.Column(db.Text)
    endpoint_id = db.Column(db.Text, db.ForeignKey("mid.journal.repository_id"))
    evidence = db.Column(db.Text)
    updated = db.Column(db.Text)
    doi = db.Column(db.Text)  # it is possible for any location to have its own doi

    @property
    def is_oa(self):
        return self.version is not None

    @property
    def display_license(self):
        if not self.license:
            return None
        elif 'publisher-specific, author manuscript' in self.license.lower():
            # manual override; this should affect only a few works, and this whole table should be deprecated soon
            return 'publisher-specific-oa'
        elif 'unspecified-oa' in self.license.lower():
            return 'other-oa'
        return self.license.lower().split(":", 1)[0]

    @property
    def display_license_id(self):
        displayed_license = self.display_license
        return f"https://openalex.org/licenses/{displayed_license}"

    @property
    def display_host_type(self):
        if self.host_type == "publisher":
            return "journal"
        return self.host_type

    @property
    def score(self):
        score = 0
        if self.version == "publishedVersion":
            score += 10000
        if self.version == "acceptedVersion":
            score += 5000
        if self.version == "submittedVersion":
            score += 1000
        if self.license:
            score += (500 - len(self.license))  # shorter is better, fewer restrictions
        if self.host_type == "publisher":
            score += 200
        if "doi.org/" in self.source_url:
            score += 100
        if ("pdf" in self.source_url) or (self.url_for_pdf is not None):
            score += 50
        if ("europepmc" in self.source_url) or ("ncbi.nlm.nih.gov" in self.source_url):
            score += 25
        return score

    @property
    def doi_url(self):
        if not self.doi:
            return None
        return "https://doi.org/{}".format(self.doi.lower())

    def has_any_url(self):
        return bool(self.url_for_pdf or self.url_for_landing_page or self.source_url)

    def is_from_unpaywall(self):
        return self.host_type is not None

    def to_dict(self, return_level="full"):
        id = None
        display_name = self.repository_institution
        publisher = None
        issn_l = None
        issn = []

        if self.journal:
            id = self.journal.openalex_id
            display_name = self.journal.display_name
            publisher = self.journal.publisher
            issn_l = self.journal.issn_l
            issn = json.loads(self.journal.issns) if self.journal.issns else []
        elif self.host_type == "publisher" and self.work.journal:
                id = self.work.journal.openalex_id
                display_name = self.work.journal.display_name
                publisher = self.work.journal.publisher
                issn_l = self.work.journal.issn_l
                issn = json.loads(self.work.journal.issns) if self.work.journal.issns else []

        response = {
            "id": id,
            "issn_l": issn_l,
            "issn": issn,
            "display_name": display_name,
            "publisher": publisher,
            "type": self.display_host_type,
            "url": self.source_url,
            "is_oa": self.is_oa,
            "version": self.version,
            "license": self.display_license,
            "license_id": self.display_license_id,
            "doi": self.doi_url,
        }

        return response

    def to_locations_dict(self):
        return {
            'source': self.journal and self.journal.to_dict(return_level='minimum'),
            'pdf_url': self.url_for_pdf,
            'landing_page_url': self.url_for_landing_page or self.source_url,
            'is_oa': self.is_oa,
            'version': self.version,
            'is_accepted': is_accepted(self.version),
            'is_published': is_published(self.version),
            'license': self.display_license,
            'license_id': self.display_license_id,
            'doi': self.doi_url,
        }

    def __repr__(self):
        return "<Location ( {} ) {}>".format(self.paper_id, self.source_url)


