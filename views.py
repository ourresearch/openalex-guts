from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask_restx import Api
from flask_restx import Resource
from flask_restx import fields
from flask_restx import inputs
from sqlalchemy.sql import func
from sqlalchemy.orm import load_only
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import or_

import json
import os
import sys
import re
import requests

from app import app
from app import db
from app import get_db_connection
from app import get_db_cursor
from app import logger
import models

from util import clean_doi
from util import is_doi
from util import is_issn
from util import jsonify_fast_no_sort
from util import str2bool
from util import TimingMessages
from util import NoDoiException

app_api = Api(app=app, version="0.0.1", title="OpenAlex", description="OpenAlex APIs", url_scheme="http", catch_all_404s=True, license="MIT", license_url="https://github.com/ourresearch/openalex-guts/blob/main/LICENSE")
base_api_endpoint = app_api.namespace("about", description="Base endpoint")
record_api_endpoint = app_api.namespace("record", description="A RecordThresher record")
work_api_endpoint = app_api.namespace("work", description="An OpenAlex work")
author_api_endpoint = app_api.namespace("author", description="An OpenAlex author")
institution_api_endpoint = app_api.namespace("institution", description="An OpenAlex institution")
journal_api_endpoint = app_api.namespace("journal", description="An OpenAlex journal")
concept_api_endpoint = app_api.namespace("concept", description="An OpenAlex concept")


def json_dumper(obj):
    """
    if the obj has a to_dict() function we've implemented, uses it to get dict.
    from http://stackoverflow.com/a/28174796
    """
    try:
        return obj.to_dict()
    except AttributeError:
        return obj.__dict__


def json_resp(thing):
    json_str = json.dumps(thing, sort_keys=True, default=json_dumper, indent=4)

    if request.path.endswith(".json") and (os.getenv("FLASK_DEBUG", False) == "True"):
        logger.info("rendering output through debug_api.html template")
        resp = make_response(render_template(
            'debug_api.html',
            data=json_str))
        resp.mimetype = "text/html"
    else:
        resp = make_response(json_str, 200)
        resp.mimetype = "application/json"
    return resp


def abort_json(status_code, msg):
    body_dict = {
        "HTTP_status_code": status_code,
        "message": msg,
        "error": True
    }
    resp_string = json.dumps(body_dict, sort_keys=True, indent=4)
    resp = make_response(resp_string, status_code)
    resp.mimetype = "application/json"
    abort(resp)



@app.after_request
def after_request_stuff(resp):

    #support CORS
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers["Access-Control-Allow-Headers"] = "Origin, X-Requested-With, Content-Type, Accept, Authorization, Cache-Control"
    resp.headers["Access-Control-Expose-Headers"] = "Authorization, Cache-Control"
    resp.headers["Access-Control-Allow-Credentials"] = "true"

    # make not cacheable because the GETs change after parameter change posts!
    resp.cache_control.max_age = 0
    resp.cache_control.no_cache = True

    # without this jason's heroku local buffers forever
    sys.stdout.flush()

    return resp


@app_api.errorhandler(NoResultFound)
def handle_no_result_exception(error):
    '''Return a custom not found error message and 404 status code'''
    return {'message': error.specific}, 404


# use this to play with the swagger CSS
@app.route('/swaggerui/swagger-ui.css')
def custom_css_theme():
    from flask import send_file
    return send_file('static/swagger-ui.css')


#### Base

@base_api_endpoint.route("/")
class BaseEndpoint(Resource):
    def get(self):
        return jsonify_fast_no_sort({
                "msg": "Welcome to OpenAlex Guts. Don't panic"
            })


#### Record

@app_api.deprecated
@record_api_endpoint.route("/RANDOM")
class RecordRandomEndpoint(Resource):
    def get(self, record_id):
        obj = models.Record.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@app_api.deprecated
@record_api_endpoint.route("/id/<string:record_id>")
class RecordIdEndpoint(Resource):
    def get(self, record_id):
        return jsonify_fast_no_sort(models.record_from_id(record_id).to_dict())



#### Work



class BigInteger(fields.Integer, fields.Raw):
    __schema_type__ = "long"
    __schema_example__ = 123456789123456789

class Doi(fields.Integer, fields.Raw):
    __schema_type__ = "doi"
    __schema_example__ = "10.123/abc.def"

my_other_model = app_api.model('Author', {
    'author_id': BigInteger(required=True),
    'doi': Doi(required=True),
})

my_model = app_api.model('Work', {
    'name': fields.String(description='The name', required=True),
    'type': fields.String(description='The object type', enum=['A', 'B']),
    'age': fields.Integer(),
    "locations": fields.List(fields.Nested(my_other_model)),
    "one_location": fields.Nested(my_other_model)
})



@work_api_endpoint.route("/RANDOM")
class WorkRandomEndpoint(Resource):
    def get(self):
        my_timing = TimingMessages()
        response = {"_timing": None}
        work_id = db.session.query(models.Work.paper_id).filter(or_(models.Work.doc_type == None, models.Work.doc_type != 'Patent')).order_by(func.random()).first()
        work_id = work_id[0]
        my_timing.log_timing("after random()")
        my_obj = models.work_from_id(work_id)
        my_timing.log_timing("after work_from_id()")
        if not my_obj:
            abort(404)
        return_level = "full"
        if ("return" in request.args) and (request.args.get("return", "full").lower() == "elastic"):
            return_level = "elastic"
        response["results"] = my_obj.to_dict(return_level)
        my_timing.log_timing("after to_dict()")
        response["_timing"] = my_timing.to_dict()
        return jsonify_fast_no_sort(response)


@work_api_endpoint.route("/id/<int:work_id>")
@app_api.doc(params={
                'id': {'description': 'id of the work (eg 2741809807)', 'in': 'path', 'type': int},
                'return': {'description': 'full or elastic (default full)', "required":False, 'in': 'query', 'type': str},
                },
             description= "An endpoint to get work from the id")
@app_api.response(200, 'Success', my_model)
@app_api.response(400, 'Validation Error')
@app_api.response(404, 'Not found')
class WorkIdEndpoint(Resource):
    def get(self, work_id):
        my_timing = TimingMessages()
        response = {"_timing": None}
        my_obj = models.work_from_id(work_id)
        my_timing.log_timing("after work_from_id()")
        if not my_obj:
            abort(404)
        return_level = "full"
        if ("return" in request.args) and (request.args.get("return", "full").lower() == "elastic"):
            return_level = "elastic"
        response["results"] = my_obj.to_dict(return_level)
        my_timing.log_timing("after to_dict()")
        response["_timing"] = my_timing.to_dict()
        return jsonify_fast_no_sort(response)

@work_api_endpoint.route("/doi/<path:doi>")
@app_api.doc(params={
                'doi': {'description': 'new DOI of the work (eg 10.7717/peerj.4375)', 'in': 'path', 'type': Doi},
                'return': {'description': 'full or elastic (default full)', "required":False, 'in': 'query', 'type': str},
                },
             description= "An endpoint to get work from the doi")
@app_api.response(200, 'Success', my_model)
@app_api.response(400, 'Validation Error')
@app_api.response(404, 'Not found')
class WorkDoiEndpoint(Resource):
    def get(self, doi):
        from util import normalize_doi
        clean_doi = normalize_doi(doi)
        my_timing = TimingMessages()
        response = {"_timing": None}
        my_obj = models.work_from_doi(clean_doi)
        my_timing.log_timing("after work_from_doi()")
        if not my_obj:
            abort(404)
        return_level = "full"
        if ("return" in request.args) and (request.args.get("return", "full").lower() == "elastic"):
            return_level = "elastic"
        response["results"] = my_obj.to_dict(return_level)
        my_timing.log_timing("after to_dict()")
        response["_timing"] = my_timing.to_dict()
        return jsonify_fast_no_sort(response)

@work_api_endpoint.route("/pmid/<string:pmid>")
class WorkPmidEndpoint(Resource):
    def get(self, pmid):
        return jsonify_fast_no_sort(models.work_from_pmid(pmid).to_dict())


#### Author

@author_api_endpoint.route("/RANDOM")
class AuthorRandomEndpoint(Resource):
    def get(self):
        obj = models.Author.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@author_api_endpoint.route("/id/<int:author_id>")
class AuthorIdEndpoint(Resource):
    def get(self, author_id):
        return jsonify_fast_no_sort(models.author_from_id(author_id).to_dict())

@author_api_endpoint.route("/orcid/<string:orcid>")
class AuthorOrcidEndpoint(Resource):
    def get(self, orcid):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.authors_from_orcid(orcid)])

@author_api_endpoint.route("/normalized_name/<string:normalized_name>")
class AuthorNormalizedNameEndpoint(Resource):
    def get(self, normalized_name):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.authors_from_normalized_name(normalized_name)])


#### Institution

@institution_api_endpoint.route("/RANDOM")
class InstitutionRandomEndpoint(Resource):
    def get(self):
        obj = models.Institution.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@institution_api_endpoint.route("/id/<int:institution_id>")
class InstitutionIdEndpoint(Resource):
    def get(self, institution_id):
        obj = models.institution_from_id(institution_id)
        if not obj:
            return abort_json(404, "not found"), 404
        return jsonify_fast_no_sort(obj.to_dict())

@institution_api_endpoint.route("/ror/<string:ror_id>")
class InstitutionRorEndpoint(Resource):
    def get(self, ror_id):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.institutions_from_ror(ror_id)])

@institution_api_endpoint.route("/grid/<string:grid_id>")
class InstitutionRorEndpoint(Resource):
    def get(self, grid_id):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.institutions_from_grid(grid_id)])


#### Journal

@journal_api_endpoint.route("/RANDOM")
class JournalRandomEndpoint(Resource):
    def get(self):
        obj = models.Journal.query.order_by(func.random()).first()
        if not obj:
            raise NoResultFound
        response = obj.to_dict()
        return response

@journal_api_endpoint.route("/id/<int:journal_id>")
@app_api.response(200, 'Success', my_model)
@app_api.response(400, 'Validation Error')
@app_api.response(404, 'Not found')
class JournalIdEndpoint(Resource):
    def get(self, journal_id):
        return jsonify_fast_no_sort(models.journal_from_id(journal_id).to_dict())

@journal_api_endpoint.route("/issn/<string:issn>")
class JournalIssnEndpoint(Resource):
    def get(self, issn):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.journals_from_issn(issn)])


#### Concept

@concept_api_endpoint.route("/RANDOM")
class ConceptRandomEndpoint(Resource):
    def get(self):
        obj = models.Concept.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@concept_api_endpoint.route("/id/<int:concept_id>")
class ConceptIdEndpoint(Resource):
    def get(self, concept_id):
        return jsonify_fast_no_sort(models.concept_from_id(concept_id).to_dict())





if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5007))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)

if False:
    print("""
    PATH=$(pyenv root)/shims:$PATH; unset PYTHONPATH
    echo 'PATH=$(pyenv root)/shims:$PATH' >> ~/.zshrc
    /Users/hpiwowar/.pyenv/versions/3.9.5/bin/python3  --version
    PYTHONPATH=/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages:
    python --version
    """)
    # unset PYTHONPATH

