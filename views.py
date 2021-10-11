from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify
from flask_restx import Api
from flask_restx import Resource

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

from util import clean_doi
from util import is_doi
from util import is_issn
from util import jsonify_fast_no_sort
from util import str2bool
from util import Timer
from util import NoDoiException

app_api = Api(app=app, version="0.0.1", description="OpenAlex APIs")
name_space_api_base = app_api.namespace("base", description="Base endpoint")
name_space_api_record = app_api.namespace("record", description="a record")
name_space_api_work = app_api.namespace("work", description="a work")


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
    resp.headers['Access-Control-Allow-Origin'] = "*"
    resp.headers['Access-Control-Allow-Methods'] = "POST, GET, OPTIONS, PUT, DELETE, PATCH"
    resp.headers['Access-Control-Allow-Headers'] = "origin, content-type, accept, x-requested-with"

    # # remove session
    # db.session.remove()

    # without this jason's heroku local buffers forever
    sys.stdout.flush()

    return resp


@name_space_api_base.route("/")
class BaseEndpoint(Resource):
    def get(self):
        return jsonify_fast_no_sort({
                "msg": "Welcome to OpenAlex Guts. Don't panic"
            })

@name_space_api_record.route("/record/<int:record_id>")
class RecordEndpoint(Resource):
    def get(self, record_id):
        return record_from_id(record_id)

@name_space_api_work.route("/work/doi/<string:doi>")
class WorkDoiEndpoint(Resource):
    def get(self, doi):
        return work_from_doi(doi)

@name_space_api_work.route("/work/pmid/<string:pmid>")
class WorkPmidEndpoint(Resource):
    def get(self, pmid):
        return work_from_pmid(pmid)


def record_from_id(record_id):
    return {"id": record_id}

def work_from_doi(doi):
    return {"id": doi}

def work_from_pmid(pmid):
    return {"id": pmid}



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

