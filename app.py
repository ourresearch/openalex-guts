from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_compress import Compress
from flask_debugtoolbar import DebugToolbarExtension
import datetime
import shortuuid

from sqlalchemy import exc
from sqlalchemy import event
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import Pool

import logging
import sys
import os
import requests
import json
import random
import warnings
from urllib.parse import urlparse
import psycopg2
import psycopg2.extras # needed though you wouldn't guess it
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import re
from collections import defaultdict

from util import safe_commit
from util import elapsed
from util import HTTPMethodOverrideMiddleware

HEROKU_APP_NAME = "openalex-guts"
USER_AGENT = "OpenAlex/0.1 (https://openalex.org; team@ourresearch.org)"

# set up logging
# see http://wiki.pylonshq.com/display/pylonscookbook/Alternative+logging+configuration

log_level = os.environ.get('LOG_LEVEL', 'INFO')

logging.basicConfig(
    stream=sys.stdout,
    level=log_level,
    format='%(thread)d: %(message)s'  #tried process but it was always "6" on heroku
)
logger = logging.getLogger("oadoi")

REDIS_URL = os.getenv("REDISCLOUD_URL")
API_HOST = os.getenv("API_HOST")
MAX_MAG_ID = 4200000000

libraries_to_mum = [
    "requests",
    "urllib3",
    "requests.packages.urllib3",
    "requests_oauthlib",
    "stripe",
    "oauthlib",
    "boto",
    "boto3",
    "botocore",
    "newrelic",
    "RateLimiter",
    "paramiko",
    "chardet",
    "cryptography",
    "psycopg2",
    "s3_concat",
    "s3transfer",
]


for a_library in libraries_to_mum:
    the_logger = logging.getLogger(a_library)
    the_logger.setLevel(logging.WARNING)
    the_logger.propagate = True
    warnings.filterwarnings("ignore", category=UserWarning, module=a_library)

# disable extra warnings
requests.packages.urllib3.disable_warnings()
warnings.filterwarnings("ignore", category=DeprecationWarning)

app = Flask(__name__)

# database stuff
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True  # as instructed, to suppress warning

app.config['SQLALCHEMY_ECHO'] = (os.getenv("SQLALCHEMY_ECHO", False) == "True")
# app.config['SQLALCHEMY_ECHO'] = True

app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("POSTGRES_URL")

database_url = urlparse(os.getenv("POSTGRES_URL"))
# database_url = urlparse(os.getenv("POSTGRES_URL))
app.config['postgreSQL_pool'] = ThreadedConnectionPool(2, 5,
                                  database=database_url.path[1:],
                                  user=database_url.username,
                                  password=database_url.password,
                                  host=database_url.hostname,
                                  port=database_url.port)

# see https://stackoverflow.com/questions/43594310/redshift-sqlalchemy-long-query-hangs
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = { "pool_pre_ping": True,
                                            "pool_recycle": 300,
                                            "connect_args": {
                                                "keepalives": 1,
                                                "keepalives_idle": 10,
                                                "keepalives_interval": 2,
                                                "keepalives_count": 5
                                            }
            }

# from http://stackoverflow.com/a/12417346/596939
# class NullPoolSQLAlchemy(SQLAlchemy):
#     def apply_driver_hacks(self, app, info, options):
#         options['poolclass'] = NullPool
#         return super(NullPoolSQLAlchemy, self).apply_driver_hacks(app, info, options)
#
# db = NullPoolSQLAlchemy(app, session_options={"autoflush": False})

app.config["SQLALCHEMY_POOL_SIZE"] = 10
db = SQLAlchemy(app, session_options={"autoflush": False, "autocommit": False})

# do compression.  has to be above flask debug toolbar so it can override this.
compress_json = os.getenv("COMPRESS_DEBUG", "True")=="True"


# set up Flask-DebugToolbar
if (os.getenv("FLASK_DEBUG", False) == "True"):
    logger.info(u"Setting app.debug=True; Flask-DebugToolbar will display")
    compress_json = False
    app.debug = True
    app.config['DEBUG'] = True
    app.config["DEBUG_TB_INTERCEPT_REDIRECTS"] = False
    app.config["SQLALCHEMY_RECORD_QUERIES"] = True
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY")
    toolbar = DebugToolbarExtension(app)

# gzip responses
Compress(app)
app.config["COMPRESS_DEBUG"] = compress_json


def new_db_connection(readonly=True):
    connection = psycopg2.connect(dbname=database_url.path[1:],
                                  user=database_url.username,
                                  password=database_url.password,
                                  host=database_url.hostname,
                                  port=database_url.port)
    connection.set_session(readonly=readonly, autocommit=True)
    return connection

readonly_connection = new_db_connection(readonly=True)
readwrite_connection = new_db_connection(readonly=False)

@contextmanager
def get_db_cursor(readonly=True):
    connection = readwrite_connection
    if readonly:
        connection = readonly_connection
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
      yield cursor
    finally:
      cursor.close()
      pass

def get_apiurl_from_openalex_url(openalex_url):
    if not openalex_url:
        return None
    return re.sub("https://openalex.org/(?P<id>[A-Za-z\d]{3,})", "http://localhost:5007/\g<id>?apiurls", openalex_url)


# reserved_openalex_ids = defaultdict(list)
#
# def get_next_openalex_id(entity):
#     global reserved_openalex_ids
#
#     try:
#         my_id = reserved_openalex_ids[entity].pop()
#     except IndexError:
#         reserved_openalex_ids[entity] = reserve_more_openalex_ids(entity, 100)
#         my_id = reserved_openalex_ids[entity].pop()
#     return my_id
#
# def reserve_more_openalex_ids(entity, number_to_reserve=100):
#     print(f"getting {number_to_reserve} more {entity} openalex ids")
#     values_string_list = []
#     assigned_labels = []
#     for i in range(0, number_to_reserve):
#         assigned_label = "{}_{}".format(datetime.datetime.utcnow().isoformat(), shortuuid.uuid()[0:10])
#         assigned_labels += [assigned_label]
#         values_string_list += [f"(now(), '{assigned_label}', '{entity}')"]
#
#     values_string = ",".join(values_string_list)
#     text_query_pattern_select = f"""
#         insert into mid.openalex_assigned_ids (assigned, assigned_label, entity) values {values_string};
#         commit;
#         select id from mid.openalex_assigned_ids where assigned_label in %s order by id desc; """
#     # text_query_pattern_select = f"""
#     #     begin transaction read write;
#     #     lock mid.openalex_assigned_ids;
#     #     insert into mid.openalex_assigned_ids (assigned, assigned_label, entity) values {values_string};
#     #     commit;
#     #     end;
#     #     select id from mid.openalex_assigned_ids where assigned_label in %s; """
#     with get_db_cursor(readonly=False) as cur:
#         # print(cur.mogrify(text_query_pattern_select, (tuple(assigned_labels), )))
#         cur.execute(text_query_pattern_select, (tuple(assigned_labels),))
#         rows = cur.fetchall()
#         ids = [row["id"] for row in rows]
#     return ids

