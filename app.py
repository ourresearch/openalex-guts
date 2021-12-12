from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_compress import Compress
from flask_debugtoolbar import DebugToolbarExtension

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

from util import safe_commit
from util import elapsed
from util import HTTPMethodOverrideMiddleware

HEROKU_APP_NAME = "openalex-guts"
USER_AGENT = {"User-Agent": "OpenAlex/0.1 (https://openalex.org; team@ourresearch.org)"}

# set up logging
# see http://wiki.pylonshq.com/display/pylonscookbook/Alternative+logging+configuration
logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(thread)d: %(message)s'  #tried process but it was always "6" on heroku
)
logger = logging.getLogger("oadoi")

API_HOST = os.getenv("API_HOST")

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

# app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL")  # don't use this though, default is unclear, use binds
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL_OPENALEX_REDSHIFT")  # don't use this though, default is unclear, use binds
app.config["SQLALCHEMY_BINDS"] = {
    "unpaywall_db": os.getenv("DATABASE_URL_UNPAYWALL"),
    "redshift_db": os.getenv("DATABASE_URL_OPENALEX_REDSHIFT")
}

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


redshift_url = urlparse(os.getenv("DATABASE_URL_OPENALEX_REDSHIFT"))
app.config['postgreSQL_pool'] = ThreadedConnectionPool(2, 5,
                                  database=redshift_url.path[1:],
                                  user=redshift_url.username,
                                  password=redshift_url.password,
                                  host=redshift_url.hostname,
                                  port=redshift_url.port)



@contextmanager
def get_db_connection():
    try:
        connection = app.config['postgreSQL_pool'].getconn()
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        connection.autocommit=True
        # connection.readonly = True
        yield connection
    finally:
        app.config['postgreSQL_pool'].putconn(connection)

@contextmanager
def get_db_cursor(commit=False):
    with get_db_connection() as connection:
      cursor = connection.cursor(
                  cursor_factory=psycopg2.extras.RealDictCursor)
      try:
          yield cursor
          if commit:
              connection.commit()
      finally:
          cursor.close()
          pass
