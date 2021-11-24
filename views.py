from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import send_from_directory
from flask import send_file
from flask import safe_join
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
import yaml

from app import app
from app import db
from app import logger
import models
import views_doc_definitions as doc
from views_doc_definitions import app_api

from util import jsonify_fast_no_sort
from util import TimingMessages

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

@app_api.route('/swagger.yml')
@app_api.hide
class Yaml(Resource):
    def get(self):
       data = json.loads(json.dumps(app_api.__schema__))
       with open('yamldoc.yml', 'w') as yamlf:
            yaml.dump(data, yamlf, allow_unicode=True, default_flow_style=False)
            file = os.path.abspath(os.getcwd())
            # return send_file(safe_join(file, 'yamldoc.yml'), as_attachment=True, attachment_filename='yamldoc.yml', mimetype='application/x-yaml')
            return send_file(safe_join(file, 'yamldoc.yml'), mimetype='text/plain')



#### Work


@doc.work_api_endpoint.route("/RANDOM")
@doc.work_api_endpoint.hide
@app_api.doc(description= "An endpoint to get a random work, for exploration and testing")
@app_api.response(200, 'Success', doc.WorkModel)
class WorkRandom(Resource):
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


@doc.work_api_endpoint.route("/id/<int:work_id>")
@app_api.doc(params={'work_id': {'description': 'OpenAlex id of the work (eg 2741809807)', 'in': 'path', 'type': doc.PaperIdModel}},
             description="An endpoint to get work from the id")
@app_api.response(200, 'Success', doc.WorkModel)
@app_api.response(404, 'Not found')
class WorkId(Resource):
    def get(self, work_id):
        my_timing = TimingMessages()
        response = {"_timing": None}

        COMPUTE_RESULT = False
        if COMPUTE_RESULT:
            my_obj = models.work_from_id(work_id)
            my_timing.log_timing("after work_from_id()")
            if not my_obj:
                abort(404)
            response["results"] = my_obj.to_dict()
        else:
            from sqlalchemy import text
            q = """select json_elastic 
                from mid.work_json
                where paper_id = :work_id;"""
            row = db.session.execute(text(q), {"work_id": work_id}).first()
            response["results"] = json.loads(row["json_elastic"])

        my_timing.log_timing("after to_dict()")
        response["_timing"] = my_timing.to_dict()
        return jsonify_fast_no_sort(response)

@doc.work_api_endpoint.route("/doi/<path:doi>")
@app_api.doc(params={'doi': {'description': 'DOI of the work (eg 10.7717/peerj.4375)', 'in': 'path', 'type': doc.DoiModel}},
             description="An endpoint to get work from the doi")
@app_api.response(200, 'Success', doc.WorkModel)
@app_api.response(404, 'Not found')
class WorkDoi(Resource):
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

@doc.work_api_endpoint.route("/pmid/<string:pmid>")
@app_api.doc(params={'pmid': {'description': 'PMID of the work (eg 21801268)', 'in': 'path', 'type': doc.PmidModel}},
             description="An endpoint to get work from the PubMed ID")
@app_api.response(200, 'Success', doc.WorkModel)
@app_api.response(404, 'Not found')
class WorkPmid(Resource):
    def get(self, pmid):
        response = models.work_from_pmid(pmid)
        if not response:
            abort(404)
        return jsonify_fast_no_sort(response.to_dict())


#### Author

@doc.author_api_endpoint.route("/RANDOM")
@doc.author_api_endpoint.hide
@app_api.doc(description= "An endpoint to get a random author, for exploration and testing")
@app_api.response(200, 'Success', doc.AuthorModel)
class AuthorRandom(Resource):
    def get(self):
        obj = models.Author.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@doc.author_api_endpoint.route("/id/<int:author_id>")
@app_api.doc(params={'author_id': {'description': 'Author ID', 'in': 'path', 'type': doc.AuthorIdModel},},
             description="An endpoint to get an author from the author_id")
@app_api.response(200, 'Success', doc.AuthorModel)
@app_api.response(404, 'Not found')
class AuthorId(Resource):
    def get(self, author_id):
        return jsonify_fast_no_sort(models.author_from_id(author_id).to_dict())

@doc.author_api_endpoint.route("/orcid/<string:orcid>")
@app_api.doc(params={'orcid': {'description': 'ORCID of the author (eg 0000-0002-6133-2581)', 'in': 'path', 'type': doc.OrcidModel},},
             description="An endpoint to get an author from the orcid")
@app_api.response(200, 'Success', doc.AuthorModel)
@app_api.response(404, 'Not found')
class AuthorOrcid(Resource):
    def get(self, orcid):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.authors_from_orcid(orcid)])


# #### Institution

@doc.institution_api_endpoint.route("/RANDOM")
@doc.institution_api_endpoint.hide
@app_api.doc(description= "An endpoint to get a random institution, for exploration and testing")
@app_api.response(200, 'Success', doc.InstitutionModel)
class InstitutionRandom(Resource):
    def get(self):
        obj = models.Institution.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@doc.institution_api_endpoint.route("/id/<int:institution_id>")
@app_api.doc(params={'institution_id': {'description': 'OpenAlex id of the institution', 'in': 'path', 'type': doc.InstitutionIdModel}},
             description="An endpoint to get institution from the id")
@app_api.response(200, 'Success', doc.InstitutionModel)
@app_api.response(404, 'Not found')
class InstitutionId(Resource):
    def get(self, institution_id):
        obj = models.institution_from_id(institution_id)
        if not obj:
            return abort_json(404, "not found"), 404
        return jsonify_fast_no_sort(obj.to_dict())

@doc.institution_api_endpoint.route("/ror/<string:ror_id>")
@app_api.doc(params={'ror_id': {'description': 'ROR id of the institution', 'in': 'path', 'type': doc.RorIdModel}},
             description="An endpoint to get institution from the ROR id")
@app_api.response(200, 'Success', doc.InstitutionModel)
@app_api.response(404, 'Not found')
class InstitutionRor(Resource):
    def get(self, ror_id):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.institutions_from_ror(ror_id)])


#### Journal

@doc.journal_api_endpoint.route("/RANDOM")
@doc.journal_api_endpoint.hide
@app_api.doc(description= "An endpoint to get a random journal, for exploration and testing")
@app_api.response(200, 'Success', doc.JournalModel)
class JournalRandom(Resource):
    def get(self):
        obj = models.Journal.query.order_by(func.random()).first()
        if not obj:
            raise NoResultFound
        response = obj.to_dict()
        return response

@doc.journal_api_endpoint.route("/id/<int:journal_id>")
@app_api.doc(params={'journal_id': {'description': 'OpenAlex id of the journal', 'in': 'path', 'type': doc.JournalIdModel}},
             description="An endpoint to get journal from the id")
@app_api.response(200, 'Success', doc.JournalModel)
@app_api.response(404, 'Not found')
class JournalId(Resource):
    def get(self, journal_id):
        return jsonify_fast_no_sort(models.journal_from_id(journal_id).to_dict())

@doc.journal_api_endpoint.route("/issn/<string:issn>")
@app_api.doc(params={'issn': {'description': 'ISSN of the journal', 'in': 'path', 'type': doc.IssnModel}},
             description="An endpoint to get journal from an ISSN")
@app_api.response(200, 'Success', doc.JournalModel)
@app_api.response(404, 'Not found')
class JournalIssn(Resource):
    def get(self, issn):
        return jsonify_fast_no_sort([obj.to_dict() for obj in models.journals_from_issn(issn)])


#### Concept

@doc.concept_api_endpoint.route("/RANDOM")
@doc.concept_api_endpoint.hide
@app_api.doc(description= "An endpoint to get a random concept, for exploration and testing")
@app_api.response(200, 'Success', doc.ConceptModel)
class ConceptRandom(Resource):
    def get(self):
        obj = models.Concept.query.order_by(func.random()).first()
        return jsonify_fast_no_sort(obj.to_dict())

@doc.concept_api_endpoint.route("/id/<int:concept_id>")
@app_api.doc(params={'concept_id': {'description': 'OpenAlex id of the concept', 'in': 'path', 'type': doc.ConceptIdModel}},
             description="An endpoint to get concept from the id")
@app_api.response(200, 'Success', doc.ConceptModel)
@app_api.response(404, 'Not found')
class ConceptId(Resource):
    def get(self, concept_id):
        return jsonify_fast_no_sort(models.concept_from_id(concept_id).to_dict())


@app.route('/loaderio-2dc2634ae02b4016d10e4085686d893d/')
def looderio_verification():
    response = make_response("loaderio-2dc2634ae02b4016d10e4085686d893d", 200)
    response.mimetype = "text/plain"
    return response

@app.route('/docs')
def send_api_docs():
    return send_from_directory("docs", "api_docs.html")


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

