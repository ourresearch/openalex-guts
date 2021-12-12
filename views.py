from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import send_from_directory
from flask import send_file
from flask import safe_join
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



def is_work_openalex_id(id):
    if isinstance(id, int):
        return False
    return id.upper().startswith("W")

def is_author_openalex_id(id):
    if isinstance(id, int):
        return False
    return id.upper().startswith("A")

def is_venue_openalex_id(id):
    if isinstance(id, int):
        return False
    return id.upper().startswith("V")

def is_institution_openalex_id(id):
    if isinstance(id, int):
        return False
    return id.upper().startswith("I")

def is_concept_openalex_id(id):
    if isinstance(id, int):
        return False
    return id.upper().startswith("C")

@app.route('/<string:openalex_id>')
def universal_get(openalex_id):
    if is_work_openalex_id(openalex_id):
        return works_id_get(openalex_id)
    elif is_author_openalex_id(openalex_id):
        return authors_id_get(openalex_id)
    elif is_venue_openalex_id(openalex_id):
        return venues_id_get(openalex_id)
    elif is_institution_openalex_id(openalex_id):
        return institutions_id_get(openalex_id)
    elif is_concept_openalex_id(openalex_id):
        return concepts_id_get(openalex_id)
    return {'message': "OpenAlex ID format not recognized"}, 404

@app.route('/swagger.yml')
def yaml_get():
   data = json.loads(json.dumps(app.__schema__))
   with open('yamldoc.yml', 'w') as yamlf:
        yaml.dump(data, yamlf, allow_unicode=True, default_flow_style=False)
        file = os.path.abspath(os.getcwd())
        # return send_file(safe_join(file, 'yamldoc.yml'), as_attachment=True, attachment_filename='yamldoc.yml', mimetype='application/x-yaml')
        return send_file(safe_join(file, 'yamldoc.yml'), mimetype='text/plain')




#### Work


@app.route("/works/RANDOM")
def works_random_get():
    my_timing = TimingMessages()
    work_id = db.session.query(models.Work.paper_id).order_by(func.random()).first()
    work_id = work_id[0]
    my_timing.log_timing("after random()")
    my_obj = models.work_from_id(work_id)
    my_timing.log_timing("after work_from_id()")
    if not my_obj:
        abort(404)
    response = my_obj.to_dict()
    my_timing.log_timing("after to_dict()")
    # response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(response)


@app.route("/works/id/<work_id>")
def works_id_get(work_id):
    my_timing = TimingMessages()

    if is_work_openalex_id(work_id):
        work_id = int(work_id[1:])

    if ("cached" in request.args):
        from sqlalchemy import text
        q = """select json_elastic 
            from mid.work_json
            where paper_id = :work_id;"""
        row = db.session.execute(text(q), {"work_id": work_id}).first()
        if not row:
            abort(404)
        paper_dict = json.loads(row["json_elastic"])
        paper_dict["cited_by_count"] = 42
        response = paper_dict
    else:
        my_obj = models.work_from_id(work_id)
        my_timing.log_timing("after work_from_id()")
        if not my_obj:
            abort(404)
        response = my_obj.to_dict()
    my_timing.log_timing("after to_dict()")
    # response["_timing"] = my_timing.to_dict()
    return jsonify_fast_no_sort(response)

@app.route("/works/doi/<path:doi>")
def works_get(self, doi):
    from util import normalize_doi
    clean_doi = normalize_doi(doi)
    my_timing = TimingMessages()
    my_obj = models.work_from_doi(clean_doi)
    my_timing.log_timing("after work_from_doi()")
    if not my_obj:
        abort(404)
    response = my_obj.to_dict()
    my_timing.log_timing("after to_dict()")
    return jsonify_fast_no_sort(response)


#### Author

@app.route("/authors/RANDOM")
def authors_random_get():
    obj = models.Author.query.order_by(func.random()).first()
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/authors/id/<author_id>")
def authors_id_get(author_id):
    if is_author_openalex_id(author_id):
        author_id = int(author_id[1:])
    return jsonify_fast_no_sort(models.author_from_id(author_id).to_dict())

@app.route("/authors/orcid/<string:orcid>")
def authors_orcid_get(orcid):
    return jsonify_fast_no_sort(models.author_from_orcid(orcid).to_dict())


# #### Institution

@app.route("/institutions/RANDOM")
def institutions_random_get():
    obj = models.Institution.query.order_by(func.random()).first()
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/institutions/id/<institution_id>")
def institutions_id_get(institution_id):
    if is_institution_openalex_id(institution_id):
        institution_id = int(institution_id[1:])
    obj = models.institution_from_id(institution_id)
    if not obj:
        return abort_json(404, "not found"), 404
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/institutions/ror/<string:ror_id>")
def institutions_ror_get(ror_id):
    return jsonify_fast_no_sort([obj.to_dict() for obj in models.institutions_from_ror(ror_id)])


#### Venue

@app.route("/venues/RANDOM")
def venues_random_get():
    obj = models.Venue.query.order_by(func.random()).first()
    if not obj:
        raise NoResultFound
    response = obj.to_dict()
    return response

@app.route("/venues/id/<journal_id>")
def venues_id_get(journal_id):
    if is_venue_openalex_id(journal_id):
        journal_id = int(journal_id[1:])
    obj = models.journal_from_id(journal_id)
    if not obj:
        abort(404)
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/venues/issn/<string:issn>")
def venues_issn_get(issn):
    obj = models.journal_from_issn(issn)
    if not obj:
        abort(404)
    return jsonify_fast_no_sort(obj.to_dict())


#### Concept

@app.route("/concepts/RANDOM")
def concepts_random_get():
    obj = models.Concept.query.order_by(func.random()).first()
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/concepts/id/<concept_id>")
def concepts_id_get(concept_id):
    if is_concept_openalex_id(concept_id):
        concept_id = int(concept_id[1:])
    return jsonify_fast_no_sort(models.concept_from_id(concept_id).to_dict())

@app.route("/concepts/wikidata/<wikidata_id>")
def concepts_wikidata_get(wikidata_id):
    print("need to implement this")
    return 1/0

@app.route("/concepts/name/<string:name>")
def concepts_name_get(name):
    obj = models.concept_from_name(name)
    if not obj:
        abort(404)
    return jsonify_fast_no_sort(obj.to_dict())



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

