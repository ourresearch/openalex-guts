from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
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
GUTS_API_KEY = os.getenv("GUTS_API_KEY")

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
REDIS_QUEUE_URL = os.getenv("REDISCLOUD_CRIMSON_URL")
API_HOST = os.getenv("API_HOST")
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_EMBEDDINGS_URL = os.getenv("ELASTIC_EMBEDDINGS_URL")
MAX_MAG_ID = 4200000000
SDG_CLASSIFIER_URL = os.getenv("SDG_CLASSIFIER_URL")

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

# CORS
CORS(app, resources={r"/*": {"origins": "*"}})

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

# indexes
AUTHORS_INDEX = "authors-v13"
CONCEPTS_INDEX = "concepts-v8"
CONTINENTS_INDEX = "continents-v1"
COUNTRIES_INDEX = "countries-v1"
DOMAINS_INDEX = "domains-v2"
FIELDS_INDEX = "fields-v2"
FUNDERS_INDEX = "funders-v3"
INSTITUTIONS_INDEX = "institutions-v7"
LANGUAGES_INDEX = "languages-v1"
PUBLISHERS_INDEX = "publishers-v4"
SDGS_INDEX = "sdgs-v1"
SOURCES_INDEX = "sources-v2"
SUBFIELDS_INDEX = "subfields-v2"
TOPICS_INDEX = "topics-v3"
TYPES_INDEX = "types-v1"
WORKS_INDEX_PREFIX = "works-v23"
WORKS_INDEX = f"{WORKS_INDEX_PREFIX}-*,-*invalid-data"

# for raw affiliation matching
COUNTRIES = {
    "AD": ["Andorra"],
    "AE": ["United Arab Emirates"],
    "AF": ["Afghanistan"],
    "AG": ["Antigua and Barbuda"],
    "AI": ["Anguilla"],
    "AL": ["Albania"],
    "AM": ["Armenia"],
    "AO": ["Angola"],
    "AQ": ["Antarctica"],
    "AR": ["Argentina"],
    "AS": ["American Samoa"],
    "AT": ["Austria"],
    "AU": ["Australia"],
    "AW": ["Aruba"],
    "AX": ["Åland Islands", "Aland Islands"],
    "AZ": ["Azerbaijan"],
    "BA": ["Bosnia and Herzegovina"],
    "BB": ["Barbados"],
    "BD": ["Bangladesh"],
    "BE": ["Belgium"],
    "BF": ["Burkina Faso"],
    "BG": ["Bulgaria"],
    "BH": ["Bahrain"],
    "BI": ["Burundi"],
    "BJ": ["Benin"],
    "BL": ["Saint Barthélemy"],
    "BM": ["Bermuda"],
    "BN": ["Brunei"],
    "BO": ["Bolivia (Plurinational State of)", "Bolivia"],
    "BQ": ["Bonaire, Sint Eustatius and Saba", "Bonaire, Saint Eustatius and Saba "],
    "BR": ["Brazil", "Brasil"],
    "BS": ["Bahamas"],
    "BT": ["Bhutan"],
    "BV": ["Bouvet Island"],
    "BW": ["Botswana"],
    "BY": ["Belarus"],
    "BZ": ["Belize"],
    "CA": ["Canada"],
    "CC": ["Cocos (Keeling) Islands"],
    "CD": [
        "Congo, Democratic Republic of the",
        "Democratic Republic of the Congo",
        "Congo",
    ],
    "CF": ["Central African Republic"],
    "CG": ["Congo", "Republic of the Congo"],
    "CH": ["Switzerland", "Suiza"],
    "CI": ["Côte d'Ivoire", "Ivory Coast"],
    "CK": ["Cook Islands"],
    "CL": ["Chile"],
    "CM": ["Cameroon"],
    "CN": ["China", "Hong Kong", "People's Republic of China"],
    "CO": ["Colombia"],
    "CR": ["Costa Rica"],
    "CU": ["Cuba"],
    "CV": ["Cabo Verde"],
    "CW": ["Curaçao", "Curacao"],
    "CX": ["Christmas Island"],
    "CY": ["Cyprus"],
    "CZ": ["Czechia", "Czech Republic"],
    "DE": ["Germany", "GDR", "Deutschland"],
    "DJ": ["Djibouti"],
    "DK": ["Denmark"],
    "DM": ["Dominica"],
    "DO": ["Dominican Republic"],
    "DZ": ["Algeria"],
    "EC": ["Ecuador"],
    "EE": ["Estonia"],
    "EG": ["Egypt"],
    "EH": ["Western Sahara"],
    "ER": ["Eritrea"],
    "ES": ["Spain", "España"],
    "ET": ["Ethiopia"],
    "FI": ["Finland"],
    "FJ": ["Fiji"],
    "FK": ["Falkland Islands (Malvinas)", "Falkland Islands"],
    "FM": ["Micronesia (Federated States of)", "Micronesia"],
    "FO": ["Faroe Islands"],
    "FR": ["France"],
    "GA": ["Gabon"],
    "GB": [
        "United Kingdom of Great Britain and Northern Ireland",
        "United Kingdom",
        "U.K.",
        "UK",
    ],
    "GD": ["Grenada"],
    "GE": ["Republic of Georgia", "Tbilisi", "Batumi", "Kutaisi"],
    "GF": ["French Guiana"],
    "GG": ["Guernsey"],
    "GH": ["Ghana"],
    "GI": ["Gibraltar"],
    "GL": ["Greenland"],
    "GM": ["Gambia"],
    "GN": ["Guinea"],
    "GP": ["Guadeloupe"],
    "GQ": ["Equatorial Guinea"],
    "GR": ["Greece"],
    "GS": ["South Georgia and the South Sandwich Islands"],
    "GT": ["Guatemala"],
    "GU": ["Guam"],
    "GW": ["Guinea-Bissau"],
    "GY": ["Guyana"],
    "HM": ["Heard Island and McDonald Islands"],
    "HN": ["Honduras"],
    "HR": ["Croatia"],
    "HT": ["Haiti"],
    "HU": ["Hungary"],
    "ID": ["Indonesia"],
    "IE": ["Ireland"],
    "IL": ["Israel"],
    "IM": ["Isle of Man"],
    "IN": ["India"],
    "IO": ["British Indian Ocean Territory"],
    "IQ": ["Iraq"],
    "IR": ["Iran (Islamic Republic of)", "Iran"],
    "IS": ["Iceland"],
    "IT": ["Italy"],
    "JM": ["Jamaica"],
    "JO": ["Jordan"],
    "JP": ["Japan"],
    "KE": ["Kenya"],
    "KG": ["Kyrgyzstan"],
    "KH": ["Cambodia"],
    "KI": ["Kiribati"],
    "KM": ["Comoros"],
    "KN": ["Saint Kitts and Nevis"],
    "KP": [
        "Korea (Democratic People's Republic of)",
        "North Korea",
        "Korea, Democratic People's Republic of",
    ],
    "KR": ["Korea, Republic of", "South Korea", "Republic of Korea", "Korea"],
    "KW": ["Kuwait"],
    "XK": ["Kosovo"],
    "KY": ["Cayman Islands"],
    "KZ": ["Kazakhstan"],
    "LA": ["Lao People's Democratic Republic", "Laos"],
    "LB": ["Lebanon"],
    "LC": ["Saint Lucia"],
    "LI": ["Liechtenstein"],
    "LK": ["Sri Lanka"],
    "LR": ["Liberia"],
    "LS": ["Lesotho"],
    "LT": ["Lithuania"],
    "LU": ["Luxembourg"],
    "LV": ["Latvia"],
    "LY": ["Libya"],
    "MA": ["Morocco"],
    "MC": ["Monaco"],
    "MD": ["Moldova, Republic of", "Moldova"],
    "ME": ["Montenegro"],
    "MF": ["Saint Martin (French part)", "Saint Martin"],
    "MG": ["Madagascar"],
    "MH": ["Marshall Islands"],
    "MK": ["North Macedonia"],
    "ML": ["Mali"],
    "MM": ["Myanmar"],
    "MN": ["Mongolia"],
    "MO": ["Macao"],
    "MP": ["Northern Mariana Islands"],
    "MQ": ["Martinique"],
    "MR": ["Mauritania"],
    "MS": ["Montserrat"],
    "MT": ["Malta"],
    "MU": ["Mauritius"],
    "MV": ["Maldives"],
    "MW": ["Malawi"],
    "MX": ["Mexico", "México"],
    "MY": ["Malaysia"],
    "MZ": ["Mozambique"],
    "NA": ["Namibia"],
    "NC": ["New Caledonia"],
    "NE": ["Niger"],
    "NF": ["Norfolk Island"],
    "NG": ["Nigeria"],
    "NI": ["Nicaragua"],
    "NL": ["Netherlands"],
    "NO": ["Norway"],
    "NP": ["Nepal"],
    "NR": ["Nauru"],
    "NU": ["Niue"],
    "NZ": ["New Zealand", "NZ"],
    "OM": ["Oman"],
    "PA": ["Panama"],
    "PE": ["Peru"],
    "PF": ["French Polynesia"],
    "PG": ["Papua New Guinea"],
    "PH": ["Philippines"],
    "PK": ["Pakistan"],
    "PL": ["Poland"],
    "PM": ["Saint Pierre and Miquelon"],
    "PN": ["Pitcairn"],
    "PR": ["Puerto Rico"],
    "PS": ["Palestine, State of", "Palestinian Territory"],
    "PT": ["Portugal"],
    "PW": ["Palau"],
    "PY": ["Paraguay"],
    "QA": ["Qatar"],
    "RE": ["Réunion", "Reunion"],
    "RO": ["Romania"],
    "RS": ["Serbia"],
    "RU": ["Russian Federation", "Russia", "USSR"],
    "RW": ["Rwanda"],
    "SA": ["Saudi Arabia"],
    "SB": ["Solomon Islands"],
    "SC": ["Seychelles"],
    "SD": ["Sudan"],
    "SE": ["Sweden"],
    "SG": ["Singapore"],
    "SH": ["Saint Helena, Ascension and Tristan da Cunha"],
    "SI": ["Slovenia"],
    "SJ": ["Svalbard and Jan Mayen"],
    "SK": ["Slovakia"],
    "SL": ["Sierra Leone"],
    "SM": ["San Marino"],
    "SN": ["Senegal"],
    "SO": ["Somalia"],
    "SR": ["Suriname"],
    "SS": ["South Sudan"],
    "ST": ["Sao Tome and Principe"],
    "SV": ["El Salvador"],
    "SX": ["Sint Maarten (Dutch part)", "Sint Maarten"],
    "SY": ["Syrian Arab Republic", "Syria"],
    "SZ": ["Eswatini"],
    "TC": ["Turks and Caicos Islands"],
    "TD": ["Chad"],
    "TF": ["French Southern Territories"],
    "TG": ["Togo"],
    "TH": ["Thailand"],
    "TJ": ["Tajikistan"],
    "TK": ["Tokelau"],
    "TL": ["Timor-Leste", "Timor Leste"],
    "TM": ["Turkmenistan"],
    "TN": ["Tunisia"],
    "TO": ["Tonga"],
    "TR": ["Turkey"],
    "TT": ["Trinidad and Tobago"],
    "TV": ["Tuvalu"],
    "TW": ["Taiwan"],
    "TZ": ["Tanzania, United Republic of", "Tanzania"],
    "UA": ["Ukraine"],
    "UG": ["Uganda"],
    "UM": ["United States Minor Outlying Islands"],
    "US": [
        "United States",
        "USA",
        "United States of America",
        "Alaska",
        "Alabama",
        "Arkansas",
        "American Samoa",
        "Arizona",
        "California",
        "Colorado",
        "Connecticut",
        "Delaware",
        "Florida",
        "Georgia",
        "Guam",
        "Hawaii",
        "Iowa",
        "Idaho",
        "Illinois",
        "Indiana",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Massachusetts",
        "Maryland",
        "Maine",
        "Michigan",
        "Minnesota",
        "Missouri",
        "Mississippi",
        "Montana",
        "North Carolina",
        "North Dakota",
        "Nebraska",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "Nevada",
        "New York",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",
        "Puerto Rico",
        "Rhode Island",
        "South Carolina",
        "South Dakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Virginia",
        "Virgin Islands",
        "Vermont",
        "Washington",
        "Wisconsin",
        "West Virginia",
        "Wyoming",
    ],
    "UY": ["Uruguay"],
    "UZ": ["Uzbekistan"],
    "VA": ["Holy See", "Vatican"],
    "VC": ["Saint Vincent and the Grenadines"],
    "VE": ["Venezuela (Bolivarian Republic of)", "Venezuela"],
    "VG": ["Virgin Islands (British)", "British Virgin Islands"],
    "VI": ["Virgin Islands (U.S.)", "US Virgin Islands"],
    "VN": ["Viet Nam", "Vietnam"],
    "VU": ["Vanuatu"],
    "WF": ["Wallis and Futuna"],
    "WS": ["Samoa"],
    "YE": ["Yemen"],
    "YT": ["Mayotte"],
    "ZA": ["South Africa"],
    "ZM": ["Zambia"],
    "ZW": ["Zimbabwe"],
}


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
