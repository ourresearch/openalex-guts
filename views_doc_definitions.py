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

class PmidModel(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_format__ = "pmid"
    __schema_example__ = 21801268

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
    __schema_example__ = 6002401




InstitutionModel = app_api.model('Institution', {
    'institution_id': InstitutionIdModel,
    'display_name': fields.String(description="name of the institution"),
    'ror_id': RorIdModel,
    "country_code": fields.String(description='2-character country code')
})

AuthorModel = app_api.model('Author', {
    'author_id': fields.Integer(description='unique author ID'),
    'display_name': fields.String(description="full name of the author"),
    'orcid': OrcidModel,
    'orcid_url': OrcidUrlModel(),
    'last_known_institution_id': InstitutionIdModel,
    "last_known_institution": fields.Nested(InstitutionModel),
    "all_institutions": fields.List(InstitutionIdModel),
    "paper_count": fields.Integer(description="number of papers this author has published"),
    "citation_count": fields.Integer(description="number of times this author has been cited"),
    "papers": fields.List(PaperIdModel),
    "citations": fields.List(PaperIdModel),
    "created_date": fields.Date(),
    "updated_date": fields.Date()
})

WorkModel = app_api.model('Work', {
    'paper_id': PaperIdModel,
})

JournalModel = app_api.model('Journal', {
    'author_id': fields.Integer(description='unique author ID'),
    'display_name': fields.String(description="full name of the author"),
    'orcid': OrcidModel,
    'orcid_url': OrcidUrlModel(),
    'last_known_institution_id': InstitutionIdModel,
    "last_known_institutions": fields.List(fields.Nested(InstitutionModel)),
    "paper_count": fields.Integer(description="number of papers this author has published"),
    "citation_count": fields.Integer(description="number of times this author has been cited"),
    "created_date": fields.Date(),
    "updated_date": fields.Date()
})

ConceptModel = app_api.model('Concept', {
    'concept_id': ConceptIdModel(description='unique concept ID'),
})

