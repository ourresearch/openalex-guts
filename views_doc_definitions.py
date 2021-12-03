from flask_restx import Api
from flask_restx import Resource
from flask_restx import fields
from flask_restx import inputs

from app import app

app_api = Api(app=app, version="0.0.1", doc=False, title="OpenAlex", description="OpenAlex APIs", url_scheme="http", catch_all_404s=True, license="MIT", license_url="https://github.com/ourresearch/openalex-guts/blob/main/LICENSE")
work_api_endpoint = app_api.namespace("work", description="An OpenAlex work")
author_api_endpoint = app_api.namespace("author", description="An OpenAlex author")
institution_api_endpoint = app_api.namespace("institution", description="An OpenAlex institution")
journal_api_endpoint = app_api.namespace("journal", description="An OpenAlex journal")
concept_api_endpoint = app_api.namespace("concept", description="An OpenAlex concept")

class BigIntegerModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_example__ = 123456789123456789

class PaperIdModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_format__ = "paper_id"
    __schema_example__ = 2613086963

class DoiModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "10.XXX/XXX"
    __schema_example__ = "10.123/abc.def"

class DoiUrlModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "url"
    __schema_example__ = "https://doi.org/10.123/abc.def"

class PmidModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "pmid"
    __schema_example__ = "21801268"

class AuthorIdModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_format__ = "author_id"
    __schema_example__ = 2162757006

class AuthorDisplayNameModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "author_display_name"
    __schema_example__ = "Jane Calvo"

class OrcidModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "0000-0000-0000-0000"
    __schema_example__ = "0000-0002-6133-2581"

class OrcidUrlModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "url"
    __schema_example__ = "https://orcid.org/0000-0002-6133-2581"

class InstitutionIdModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_format__ = "institution_id"
    __schema_example__ = 6002401

class InstitutionDisplayNameModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "institution_display_name"
    __schema_example__ = "University of Examples"

class RorIdModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "ror_id"
    __schema_example__ = "001ykb961"

class RorUrlModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "url"
    __schema_example__ = "https://ror.org/001ykb961"

class JournalIdModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_format__ = "journal_id"
    __schema_example__ = 6002401

class IssnModel(fields.String, fields.Raw):
    __schema_type__ = "string"
    __schema_format__ = "0000-0000"
    __schema_example__ = "0123-345X"

class ConceptIdModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_format__ = "institution_id"
    __schema_example__ = 2778407487

class IndexWordCount(fields.Integer, fields.Raw):
    __schema_type__ = "int"
    __schema_format__ = "int"
    __schema_example__ = 42

class ConceptLevel(fields.Integer, fields.Raw):
    __schema_type__ = "int"
    __schema_format__ = "int"
    __schema_example__ = 2

AbstractWordCount = fields.Wildcard(IndexWordCount)
AbstractIndexModel = app_api.model('AbstractInvertedIndex', {
    "*": AbstractWordCount
})

AncestorConceptModel = app_api.model('AncestorConcept', {
    'id': ConceptIdModel,
    'display_name': fields.String,
    'level': ConceptLevel
})

PaperExternalIdModel = app_api.model('WorkExtraIds', {
    'doi': DoiModel,
    'doi_url': DoiUrlModel,
    'pmid': PmidModel,
    'pmid_url': fields.Url,
})

OaStatusModel = fields.String(description='Open Access status of the paper',
                                 enum=['closed', 'green', 'gold', 'hybrid', 'bronze'])

SourceDescriptionModel = fields.String(description='Type of resource at the Source URL',
                                 enum=['html', 'text', 'pdf', 'doc', 'ppt', 'xls', 'ods', 'rtf', 'xml', 'rss', 'odp', 'mp3', 'odt', 'swf', 'zip', 'ics', 'pub'])

HostTypeModel = fields.String(description='Host type',
                                 enum=['repository', 'publisher'])

VersionModel = fields.String(description='Version of the paper',
                                 enum=['submittedVersion', 'acceptedVersion', 'publishedVersion'])


LocationModel = app_api.model('Location', {
    'source_url': fields.Url,
    'source_type': fields.Integer,
    'source_description': SourceDescriptionModel,
    'language_code': fields.String,
    'url_for_landing_page': fields.String,
    'url_for_pdf': fields.String,
    'host_type': HostTypeModel,
    'version': VersionModel,
    'license': fields.String,
    'repository_institution': fields.String,
    'oai_pmh_id': fields.String,
})

MeshModel = app_api.model('Mesh', {
    'descriptor_ui': fields.String,
    'descriptor_name': fields.String,
    'qualifier_ui': fields.String,
    'qualifier_name': fields.String,
})

InstitutionModel = app_api.model('Institution', {
    'id': InstitutionIdModel,
    'display_name': InstitutionDisplayNameModel,
    'ror_id': RorIdModel,
    'ror_url': RorUrlModel,
    'grid_id': fields.String(description='GRID id (deprecated in favour of ROR id)'),
    "city": fields.String(description='city of institution'),
    "country_code": fields.String(description='2-character country code'),
    "country": fields.String(description='name of country'),
    "official_page": fields.Url(description='url of institution'),
    "wiki_page": fields.Url(description='url of Wikipedia page of institution'),
    "created_date": fields.Date(),
})

InstitutionSmallModel = app_api.model('InstitutionSmall', {
    'id': InstitutionIdModel,
    'original_affiliation': fields.String,
})

AuthorModel = app_api.model('Author', {
    'id': AuthorIdModel,
    'display_name': AuthorDisplayNameModel(description="full name of the author"),
    'orcid': OrcidModel,
    'orcid_url': OrcidUrlModel(),
    'last_known_institution_id': InstitutionIdModel,
    "last_known_institution": fields.Nested(InstitutionModel),
    "all_institutions": fields.List(InstitutionIdModel),
    "paper_count": fields.Integer(description="number of papers this author has published"),
    "citation_count": fields.Integer(description="number of times this author has been cited"),
    # "papers": fields.List(PaperIdModel),
    # "citations": fields.List(PaperIdModel),
    "created_date": fields.Date(),
    "updated_date": fields.Date()
})

AffiliationModel = app_api.model('Affiliation', {
    'author_sequence_number': fields.Integer(description="author order"),
    "institution": fields.Nested(InstitutionSmallModel),
    "author": fields.Nested(AuthorModel)
})

ConceptModel = app_api.model('Concept', {
    'id': ConceptIdModel(description='unique concept ID'),
    'display_name': fields.String(description="full name of the journal"),
    'main_type': fields.String,
    'level': ConceptLevel,
    "paper_count": fields.Integer(description="number of papers associated with this concept"),
    "citation_count": fields.Integer(description="number of times papers with this tag have been cited"),
    "ancestors": fields.List(fields.Nested(AncestorConceptModel)),
    "created_date": fields.Date(),
})


JournalModel = app_api.model('Journal', {
    'id': JournalIdModel(),
    'display_name': fields.String(description="full name of the journal"),
    'issn_l': IssnModel(description="linking ISSN for this journal"),
    'issns': fields.List(IssnModel, description="all ISSNs for this journal, print and electronic"),
    'is_oa': fields.Boolean(),
    'is_in_doaj': fields.Boolean(),
    "publisher": fields.String(),
    "paper_count": fields.Integer(description="number of papers this author has published"),
    "citation_count": fields.Integer(description="number of times this author has been cited"),
    "created_date": fields.Date(),
    "updated_date": fields.Date()
})


WorkModel = app_api.model('Work', {
    'id': PaperIdModel,
    'paper_title': fields.String(description="title of the paper"),
    'year': fields.Integer(description="year of publication"),
    'publication_date': fields.Date(description="date of publication"),
    'doc_type': fields.String(description="doc_type"),
    'genre': fields.String(description="genre"),
    'volume': fields.String(),
    'issue': fields.String(),
    'first_page': fields.String(),
    'last_page': fields.String(),
    'journal': fields.Nested(JournalModel),
    'oa_status': OaStatusModel,
    'best_version': VersionModel,
    'best_license': fields.String(),
    'best_host_type': HostTypeModel,
    'best_url': fields.Url(),
    "citation_count": fields.Integer(description="number of times this paper has been cited"),
    'ids': fields.Nested(PaperExternalIdModel),
    'affiliations': fields.List(fields.Nested(AffiliationModel)),
    'mesh': fields.List(fields.Nested(MeshModel)),
    'locations': fields.List(fields.Nested(LocationModel)),
    'citations': fields.List(PaperIdModel),
    'abstract_inverted_index': fields.Nested(AbstractIndexModel),
    'concepts': fields.List(fields.Nested(ConceptModel), description="concepts"),
    "created_date": fields.Date()
})
