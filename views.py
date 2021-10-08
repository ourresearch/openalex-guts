from flask import make_response
from flask import request
from flask import redirect
from flask import abort
from flask import render_template
from flask import jsonify

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



@app.route('/', methods=["GET", "POST"])
def base_endpoint():
    return jsonify_fast_no_sort({
        "version": "0.0.1",
        "msg": "Welcome to OpenAlex Guts. Don't panic"
    })


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

