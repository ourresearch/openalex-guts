from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask import send_from_directory
from flask import send_file
from flask import safe_join
from flask import url_for
from sqlalchemy.sql import func
from sqlalchemy.orm import load_only
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import or_
import re

import json
import os
import sys
import yaml

from app import app
from app import db
from app import logger
from app import MAX_MAG_ID
import models

from util import jsonify_fast_no_sort
from util import TimingMessages
from util import is_openalex_id
from util import normalize_openalex_id


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

@app.after_request
def after_request_override_urls_for_debugging(response):
    wants_apiurls = ("apiurls" in request.args)
    if wants_apiurls:
        json_response_data = response.get_data().decode('utf-8')
        json_response_data = re.sub("https://openalex.org/(?P<id>[A-Za-z\d]{3,})", "https://api.openalex.org/\g<id>?apiurls", json_response_data)
        response.set_data(json_response_data.encode())
    return response


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


@app.route('/swagger.yml')
def yaml_get():
   data = json.loads(json.dumps(app.__schema__))
   with open('yamldoc.yml', 'w') as yamlf:
        yaml.dump(data, yamlf, allow_unicode=True, default_flow_style=False)
        file = os.path.abspath(os.getcwd())
        # return send_file(safe_join(file, 'yamldoc.yml'), as_attachment=True, attachment_filename='yamldoc.yml', mimetype='application/x-yaml')
        return send_file(safe_join(file, 'yamldoc.yml'), mimetype='text/plain')


#### Record

@app.route('/records/RANDOM')
@app.route('/records/random')
def records_random_get():
    from models import Record
    obj = db.session.query(Record).order_by(func.random()).first()
    if not obj:
        abort(404)
    return jsonify_fast_no_sort({"n": len(obj.siblings), "siblings": obj.siblings})



@app.route('/records/<id>')
def records_id_get(id):
    from models import Record
    obj = Record.query.get(id)
    return jsonify_fast_no_sort({"n": len(obj.siblings), "siblings": obj.siblings})




#### Work


@app.route("/works/RANDOM")
@app.route("/works/random")
def works_random_get():
    query = db.session.query(models.Work.paper_id).order_by(func.random())
    if ("new" in request.args):
        query = query.filter(models.Work.paper_id >= MAX_MAG_ID)
    work_id = query.first()
    work_id = work_id[0]
    obj = models.work_from_id(work_id)
    if not obj:
        abort(404)
    response = obj.to_dict()
    return jsonify_fast_no_sort(response)


@app.route("/works/<path:id>")
def works_id_get(id):
    from util import normalize_doi
    if is_openalex_id(id):
        id = normalize_openalex_id(id)
        paper_id = int(id[1:])
        obj = models.work_from_id(paper_id)
    else:
        clean_doi = normalize_doi(id)
        if not clean_doi:
            abort(404)
        obj = models.work_from_doi(clean_doi)
    if not obj:
        abort(404)
    response = obj.to_dict()
    return jsonify_fast_no_sort(response)


#### Author

@app.route("/authors/RANDOM")
@app.route("/authors/random")
def authors_random_get():
    query = models.Author.query.order_by(func.random())
    if ("new" in request.args):
        query = query.filter(models.Author.author_id >= MAX_MAG_ID)
    obj = query.first()
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/authors/<path:id>")
def authors_id_get(id):
    from util import normalize_orcid
    if is_openalex_id(id):
        id = normalize_openalex_id(id)
        author_id = int(id[1:])
        obj = models.author_from_id(author_id)
    else:
        clean_orcid = normalize_orcid(id)
        if not clean_orcid:
            abort(404)
        obj = models.author_from_orcid(clean_orcid)
    if not obj:
        abort(404)
    response = obj.to_dict()
    return jsonify_fast_no_sort(response)



# #### Institution

@app.route("/institutions/RANDOM")
@app.route("/institutions/random")
def institutions_random_get():
    query = models.Institution.query.order_by(func.random())
    if ("new" in request.args):
        query = query.filter(models.Institution.affiliation_id >= MAX_MAG_ID)
    obj = query.first()
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/institutions/<path:id>")
def institutions_id_get(id):
    from util import normalize_ror
    if is_openalex_id(id):
        id = normalize_openalex_id(id)
        id = int(id[1:])
        obj = models.institution_from_id(id)
    else:
        clean_ror = normalize_ror(id)
        if not clean_ror:
            abort(404)
        obj = models.institution_from_ror(clean_ror)
    if not obj:
        abort(404)
    response = obj.to_dict()
    return jsonify_fast_no_sort(response)

#### Venue

@app.route("/venues/RANDOM")
@app.route("/venues/random")
def venues_random_get():
    query = models.Venue.query.order_by(func.random())
    if ("new" in request.args):
        query = query.filter(models.Venue.journal_id >= MAX_MAG_ID)
    obj = query.first()
    if not obj:
        raise NoResultFound
    response = obj.to_dict()
    return jsonify_fast_no_sort(response)

@app.route("/venues/<path:id>")
def venues_id_get(id):
    from util import normalize_issn
    if is_openalex_id(id):
        id = normalize_openalex_id(id)
        id = int(id[1:])
        obj = models.journal_from_id(id)
    else:
        clean_issn = normalize_issn(id)
        if not clean_issn:
            abort(404)
        obj = models.journal_from_issn(clean_issn)
    if not obj:
        abort(404)
    response = obj.to_dict()
    return jsonify_fast_no_sort(response)


#### Concept

@app.route("/concepts/RANDOM")
@app.route("/concepts/random")
def concepts_random_get():
    query = models.Concept.query.order_by(func.random())
    if ("new" in request.args):
        query = query.filter(models.Concept.field_of_study_id >= MAX_MAG_ID)
    obj = query.first()
    return jsonify_fast_no_sort(obj.to_dict())

@app.route("/concepts/<path:id>")
def concepts_id_get(id):
    if is_openalex_id(id):
        id = normalize_openalex_id(id)
        id = int(id[1:])
    return jsonify_fast_no_sort(models.concept_from_id(id).to_dict())

@app.route("/concepts/wikidata/<path:wikidata_id>")
def concepts_wikidata_get(wikidata_id):
    pass
    # from util import normalize_wikidata_id
    # clean_ror = normalize_ror(ror)
    # if not clean_ror:
    #     abort(404)
    # obj = models.institutions_from_ror(clean_ror)
    # if not obj:
    #     abort(404)
    # response = obj.to_dict()
    # return jsonify_fast_no_sort(response)

@app.route("/concepts/name/<string:name>")
def concepts_name_get(name):
    obj = models.concept_from_name(name)
    if not obj:
        abort(404)
    return jsonify_fast_no_sort(obj.to_dict())

######


@app.route('/<path:openalex_id>')
def universal_get(openalex_id):
    if not openalex_id:
        return {'message': "Don't panic"}, 404

    if not is_openalex_id(openalex_id):
        return {'message': "OpenAlex ID format not recognized"}, 404

    openalex_id = normalize_openalex_id(openalex_id)
    if is_work_openalex_id(openalex_id):
        return redirect(url_for("works_id_get", id=openalex_id, **request.args))
    elif is_author_openalex_id(openalex_id):
        return redirect(url_for("authors_id_get", id=openalex_id, **request.args))
    elif is_venue_openalex_id(openalex_id):
        return redirect(url_for("venues_id_get", id=openalex_id, **request.args))
    elif is_institution_openalex_id(openalex_id):
        return redirect(url_for("institutions_id_get", id=openalex_id, **request.args))
    elif is_concept_openalex_id(openalex_id):
        return redirect(url_for("concepts_id_get", id=openalex_id, **request.args))
    return {'message': "OpenAlex ID format not recognized"}, 404


@app.route('/loaderio-2dc2634ae02b4016d10e4085686d893d/')
def looderio_verification():
    response = make_response("loaderio-2dc2634ae02b4016d10e4085686d893d", 200)
    response.mimetype = "text/plain"
    return response



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

