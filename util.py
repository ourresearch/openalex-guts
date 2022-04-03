import datetime
import time
import unicodedata
import sqlalchemy
import logging
import math
import bisect
import urllib.parse
import re
import os
import collections
import requests
import heroku3
import json
import copy
from unidecode import unidecode
from sqlalchemy import sql
from sqlalchemy import exc
from subprocess import call
from requests.adapters import HTTPAdapter
import csv

def str2bool(v):
    if not v:
        return False
    return v.lower() in ("yes", "true", "t", "1")

class NoDoiException(Exception):
    pass

class NotJournalArticleException(Exception):
    pass

class DelayedAdapter(HTTPAdapter):
    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        # logger.info(u"in DelayedAdapter getting {}, sleeping for 2 seconds".format(request.url))
        # sleep(2)
        start_time = time.time()
        response = super(DelayedAdapter, self).send(request, stream, timeout, verify, cert, proxies)
        # logger.info(u"   HTTPAdapter.send for {} took {} seconds".format(request.url, elapsed(start_time, 2)))
        return response

def read_csv_file(filename):
    with open(filename, "r") as csv_file:
        my_reader = csv.DictReader(csv_file)
        rows = [row for row in my_reader]
    return rows

# from http://stackoverflow.com/a/3233356/596939
def update_recursive_sum(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update_recursive_sum(d.get(k, {}), v)
            d[k] = r
        else:
            if k in d:
                d[k] += u[k]
            else:
                d[k] = u[k]
    return d

# returns dict with values that are proportion of all values
def as_proportion(my_dict):
    if not my_dict:
        return {}
    total = sum(my_dict.values())
    resp = {}
    for k, v in my_dict.items():
        resp[k] = round(float(v)/total, 2)
    return resp

def calculate_percentile(refset, value):
    if value is None:  # distinguish between that and zero
        return None

    matching_index = bisect.bisect_left(refset, value)
    percentile = float(matching_index) / len(refset)
    # print u"percentile for {} is {}".format(value, percentile)

    return percentile

def clean_html(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext

# good for deduping strings.  warning: output removes spaces so isn't readable.
def normalize(text):
    if not text:
        return None
    response = text.lower()
    response = unidecode(str(response))
    response = clean_html(response)  # has to be before remove_punctuation
    response = remove_punctuation(response)
    response = re.sub(r"\b(a|an|the)\b", "", response)
    response = re.sub(r"\b(and)\b", "", response)
    response = re.sub("\s+", "", response)
    return response


def normalize_simple(text, remove_articles=True, remove_spaces=True):
    if not text:
        return None
    response = text.lower()
    response = remove_punctuation(response)
    if remove_articles:
        response = re.sub(r"\b(a|an|the)\b", "", response)
    if remove_spaces:
        response = re.sub("\s+", "", response)
    return response

def normalize_doi(doi, return_none_if_error=False):
    if not doi:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no DOI at all.")

    doi = doi.strip().lower()

    # test cases for this regex are at https://regex101.com/r/zS4hA0/4
    p = re.compile(r'(10\.\d+/[^\s]+)')
    matches = re.findall(p, doi)

    if len(matches) == 0:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no valid DOI.")

    doi = matches[0]

    # clean_doi has error handling for non-utf-8
    # but it's preceded by a call to remove_nonprinting_characters
    # which calls to_unicode_or_bust with no error handling
    # clean/normalize_doi takes a unicode object or utf-8 basestring or dies
    doi = to_unicode_or_bust(doi)

    return doi.replace('\0', '')


def normalize_orcid(orcid):
    if not orcid:
        return None
    orcid = orcid.strip().upper()
    p = re.compile(r'(\d{4}-\d{4}-\d{4}-\d{3}[\dX])')
    matches = re.findall(p, orcid)
    if len(matches) == 0:
        return None
    orcid = matches[0]
    orcid = orcid.replace('\0', '')
    return orcid

def normalize_pmid(pmid):
    if not pmid:
        return None
    pmid = pmid.strip().lower()
    p = re.compile('(\d+)')
    matches = re.findall(p, pmid)
    if len(matches) == 0:
        return None
    pmid = matches[0]
    pmid = pmid.replace('\0', '')
    return pmid

def normalize_ror(ror):
    if not ror:
        return None
    ror = ror.strip().lower()
    p = re.compile(r'([a-z\d]*$)')
    matches = re.findall(p, ror)
    if len(matches) == 0:
        return None
    ror = matches[0]
    ror = ror.replace('\0', '')
    return ror

def normalize_issn(issn):
    if not issn:
        return None
    issn = issn.strip().lower()
    p = re.compile("[\dx]{4}-[\dx]{4}")
    matches = re.findall(p, issn)
    if len(matches) == 0:
        return None
    issn = matches[0]
    issn = issn.replace('\0', '')
    return issn

def normalize_wikidata(wikidata):
    if not wikidata:
        return None
    wikidata = wikidata.strip().upper()
    p = re.compile("Q\d*")
    matches = re.findall(p, wikidata)
    if len(matches) == 0:
        return None
    wikidata = matches[0]
    wikidata = wikidata.replace('\0', '')
    return wikidata

def is_openalex_id(openalex_id):
    if not openalex_id:
        return False
    openalex_id = openalex_id.lower()
    if re.findall(r"http[s]://openalex.org/([waicv]\d{2,})", openalex_id):
        return True
    if re.findall(r"^([waicv]\d{2,})", openalex_id):
        return True
    if re.findall(r"(openalex:[waicv]\d{2,})", openalex_id):
        return True
    return False

def normalize_openalex_id(openalex_id):
    if not openalex_id:
        return None
    openalex_id = openalex_id.strip().upper()
    p = re.compile("([WAICV]\d{2,})")
    matches = re.findall(p, openalex_id)
    if len(matches) == 0:
        return None
    clean_openalex_id = matches[0]
    clean_openalex_id = clean_openalex_id.replace('\0', '')
    return clean_openalex_id

def remove_everything_but_alphas(input_string):
    # from http://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
    only_alphas = input_string
    if input_string:
        only_alphas = "".join(e for e in input_string if (e.isalpha()))
    return only_alphas

def remove_punctuation(input_string):
    # from http://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
    no_punc = input_string
    if input_string:
        no_punc = "".join(e for e in input_string if (e.isalnum() or e.isspace()))
    return no_punc

# from http://stackoverflow.com/a/11066579/596939
def replace_punctuation(text, sub):
    punctutation_cats = set(['Pc', 'Pd', 'Ps', 'Pe', 'Pi', 'Pf', 'Po'])
    chars = []
    for my_char in text:
        if unicodedata.category(my_char) in punctutation_cats:
            chars.append(sub)
        else:
            chars.append(my_char)
    return "".join(chars)


# from http://stackoverflow.com/a/22238613/596939
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")

def conversational_number(number):
    words = {
        "1.0": "one",
        "2.0": "two",
        "3.0": "three",
        "4.0": "four",
        "5.0": "five",
        "6.0": "six",
        "7.0": "seven",
        "8.0": "eight",
        "9.0": "nine",
    }

    if number < 1:
        return round(number, 2)

    elif number < 1000:
        return int(math.floor(number))

    elif number < 1000000:
        divided = number / 1000.0
        unit = "thousand"

    else:
        divided = number / 1000000.0
        unit = "million"

    short_number = '{}'.format(round(divided, 2))[:-1]
    if short_number in words:
        short_number = words[short_number]

    return short_number + " " + unit



def safe_commit(db):
    try:
        db.session.commit()
        return True
    except (KeyboardInterrupt, SystemExit):
        # let these ones through, don't save anything to db
        raise
    except sqlalchemy.exc.DataError:
        db.session.rollback()
        print("sqlalchemy.exc.DataError on commit.  rolling back.")
    except Exception:
        db.session.rollback()
        print("generic exception in commit.  rolling back.")
        logging.exception("commit error")
    return False


def is_pmc(url):
    return "ncbi.nlm.nih.gov/pmc" in url or "europepmc.org/articles/" in url


def is_doi(text):
    if not text:
        return False

    try_to_clean_doi = clean_doi(text, return_none_if_error=True)
    if try_to_clean_doi:
        return True
    return False

def is_issn(text):
    if not text:
        return False

    p = re.compile("[\dx]{4}-[\dx]{4}")
    matches = re.findall(p, text.lower())
    if len(matches) > 0:
        return True
    return False


def is_doi_url(url):
    if not url:
        return False

    # test urls at https://regex101.com/r/yX5cK0/2
    p = re.compile("https?:\/\/(?:dx.)?doi.org\/(.*)")
    matches = re.findall(p, url.lower())
    if len(matches) > 0:
        return True
    return False

def is_ip(ip):
    if re.match("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
        return True
    return False

def clean_doi(dirty_doi, return_none_if_error=False):
    if not dirty_doi:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no DOI at all.")

    dirty_doi = dirty_doi.strip()
    dirty_doi = dirty_doi.lower()

    # test cases for this regex are at https://regex101.com/r/zS4hA0/1
    p = re.compile(r'(10\.\d+\/[^\s]+)')

    matches = re.findall(p, dirty_doi)
    if len(matches) == 0:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no valid DOI.")

    match = matches[0]
    match = remove_nonprinting_characters(match)

    try:
        resp = str(match, "utf-8")  # unicode is valid in dois
    except (TypeError, UnicodeDecodeError):
        resp = match

    # remove any url fragments
    if "#" in resp:
        resp = resp.split("#")[0]

    # remove double quotes, they shouldn't be there as per http://www.doi.org/syntax.html
    resp = resp.replace('"', '')

    # remove trailing period, comma -- it is likely from a sentence or citation
    if resp.endswith(",") or resp.endswith("."):
        resp = resp[:-1]

    return resp


def pick_best_url(urls):
    if not urls:
        return None

    #get a backup
    response = urls[0]

    # now go through and pick the best one
    for url in urls:
        # doi if available
        if "doi.org" in url:
            response = url

        # anything else if what we currently have is bogus
        if response == "http://www.ncbi.nlm.nih.gov/pmc/articles/PMC":
            response = url

    return response

def date_as_iso_utc(datetime_object):
    if datetime_object is None:
        return None

    date_string = "{}{}".format(datetime_object, "+00:00")
    return date_string


def dict_from_dir(obj, keys_to_ignore=None, keys_to_show="all"):

    if keys_to_ignore is None:
        keys_to_ignore = []
    elif isinstance(keys_to_ignore, str):
        keys_to_ignore = [keys_to_ignore]

    ret = {}

    if keys_to_show != "all":
        for key in keys_to_show:
            ret[key] = getattr(obj, key)

        return ret


    for k in dir(obj):
        value = getattr(obj, k)

        if k.startswith("_"):
            pass
        elif k in keys_to_ignore:
            pass
        # hide sqlalchemy stuff
        elif k in ["query", "query_class", "metadata"]:
            pass
        elif callable(value):
            pass
        else:
            try:
                # convert datetime objects...generally this will fail becase
                # most things aren't datetime object.
                ret[k] = time.mktime(value.timetuple())
            except AttributeError:
                ret[k] = value
    return ret


def median(my_list):
    """
    Find the median of a list of ints

    from https://stackoverflow.com/questions/24101524/finding-median-of-list-in-python/24101655#comment37177662_24101655
    """
    my_list = sorted(my_list)
    if len(my_list) < 1:
            return None
    if len(my_list) %2 == 1:
            return my_list[((len(my_list)+1)/2)-1]
    if len(my_list) %2 == 0:
            return float(sum(my_list[(len(my_list)/2)-1:(len(my_list)/2)+1]))/2.0


def underscore_to_camelcase(value):
    words = value.split("_")
    capitalized_words = []
    for word in words:
        capitalized_words.append(word.capitalize())

    return "".join(capitalized_words)

def chunks(l, n):
    """
    Yield successive n-sized chunks from l.

    from http://stackoverflow.com/a/312464
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]

def page_query(q, page_size=1000):
    offset = 0
    while True:
        r = False
        print("util.page_query() retrieved {} things".format(page_query()))
        for elem in q.limit(page_size).offset(offset):
            r = True
            yield elem
        offset += page_size
        if not r:
            break

def elapsed(since, round_places=2):
    return round(time.time() - since, round_places)



def truncate(str, max=100):
    if len(str) > max:
        return str[0:max] + "..."
    else:
        return str


def str_to_bool(x):
    if x.lower() in ["true", "1", "yes"]:
        return True
    elif x.lower() in ["false", "0", "no"]:
        return False
    else:
        raise ValueError("This string can't be cast to a boolean.")

# from http://stackoverflow.com/a/20007730/226013
ordinal = lambda n: "%d%s" % (n,"tsnrhtdd"[(n/10%10!=1)*(n%10<4)*n%10::4])

#from http://farmdev.com/talks/unicode/
def to_unicode_or_bust(obj, encoding='utf-8'):
    if isinstance(obj, str):
        if not isinstance(obj, str):
            obj = str(obj, encoding)
    return obj

def remove_nonprinting_characters(input, encoding='utf-8'):
    input_was_unicode = True
    if isinstance(input, str):
        if not isinstance(input, str):
            input_was_unicode = False

    unicode_input = to_unicode_or_bust(input)

    # see http://www.fileformat.info/info/unicode/category/index.htm
    char_classes_to_remove = ["C", "M", "Z"]

    response = ''.join(c for c in unicode_input if unicodedata.category(c)[0] not in char_classes_to_remove)

    if not input_was_unicode:
        response = response.encode(encoding)

    return response

# getting a "decoding Unicode is not supported" error in this function?
# might need to reinstall libaries as per
# http://stackoverflow.com/questions/17092849/flask-login-typeerror-decoding-unicode-is-not-supported
class HTTPMethodOverrideMiddleware(object):
    allowed_methods = frozenset([
        'GET',
        'HEAD',
        'POST',
        'DELETE',
        'PUT',
        'PATCH',
        'OPTIONS'
    ])
    bodyless_methods = frozenset(['GET', 'HEAD', 'OPTIONS', 'DELETE'])

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        method = environ.get('HTTP_X_HTTP_METHOD_OVERRIDE', '').upper()
        if method in self.allowed_methods:
            method = method.encode('ascii', 'replace')
            environ['REQUEST_METHOD'] = method
        if method in self.bodyless_methods:
            environ['CONTENT_LENGTH'] = '0'
        return self.app(environ, start_response)


# could also make the random request have other filters
# see docs here: https://github.com/CrossRef/rest-api-doc/blob/master/rest_api.md#sample
# usage:
# dois = get_random_dois(50000, from_date="2002-01-01", only_journal_articles=True)
# dois = get_random_dois(100000, only_journal_articles=True)
# fh = open("data/random_dois_articles_100k.txt", "w")
# fh.writelines(u"\n".join(dois))
# fh.close()
def get_random_dois(n, from_date=None, only_journal_articles=True):
    dois = []
    while len(dois) < n:
        # api takes a max of 100
        number_this_round = min(n, 100)
        url = "https://api.crossref.org/works?sample={}".format(number_this_round)
        if only_journal_articles:
            url += "&filter=type:journal-article"
        if from_date:
            url += ",from-pub-date:{}".format(from_date)
        print(url)
        print("calling crossref, asking for {} dois, so far have {} of {} dois".format(
            number_this_round, len(dois), n))
        r = requests.get(url)
        items = r.json()["message"]["items"]
        dois += [item["DOI"].lower() for item in items]
    return dois


# from https://github.com/elastic/elasticsearch-py/issues/374
# to work around unicode problem
# class JSONSerializerPython2(elasticsearch.serializer.JSONSerializer):
#     """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
#     See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
#     A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
#     as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
#     """
#     def dumps(self, data):
#         # don't serialize strings
#         if isinstance(data, elasticsearch.compat.string_types):
#             return data
#         try:
#             return json.dumps(data, default=self.default, ensure_ascii=False)
#         except (ValueError, TypeError) as e:
#             raise elasticsearch.exceptions.SerializationError(data, e)



def is_the_same_url(url1, url2):
    norm_url1 = strip_jsessionid_from_url(url1.replace("https", "http"))
    norm_url2 = strip_jsessionid_from_url(url2.replace("https", "http"))
    if norm_url1 == norm_url2:
        return True
    return False

def strip_jsessionid_from_url(url):
    url = re.sub(r";jsessionid=\w+", "", url)
    return url

def get_link_target(url, base_url, strip_jsessionid=True):
    if strip_jsessionid:
        url = strip_jsessionid_from_url(url)
    if base_url:
        url = urllib.parse.urljoin(base_url, url)
    return url


def run_sql(db, q):
    q = q.strip()
    if not q:
        return
    start = time.time()
    try:
        con = db.engine.connect()
        trans = con.begin()
        con.execute(q)
        trans.commit()
    except exc.ProgrammingError as e:
        pass
    finally:
        con.close()

def get_sql_answer(db, q):
    row = db.engine.execute(sql.text(q)).first()
    return row[0]

def get_sql_answers(db, q):
    rows = db.engine.execute(sql.text(q)).fetchall()
    if not rows:
        return []
    return [row[0] for row in rows]



# from https://gist.github.com/douglasmiranda/5127251
# deletes a key from nested dict
def delete_key_from_dict(dictionary, key):
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in delete_key_from_dict(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in delete_key_from_dict(key, d):
                    yield result


def restart_dynos(app_name, dyno_prefix):
    heroku_conn = heroku3.from_key(os.getenv('HEROKU_API_KEY'))
    app = heroku_conn.apps()[app_name]
    dynos = app.dynos()
    for dyno in dynos:
        if dyno.name.startswith(dyno_prefix):
            dyno.restart()
            print("restarted {} on {}!".format(dyno.name, app_name))

def is_same_publisher(publisher1, publisher2):
    if publisher1 and publisher2:
        return normalize(publisher1) == normalize(publisher2)
    return False


from flask import current_app
from json import dumps

# from https://stackoverflow.com/a/50762571/596939
def jsonify_fast(*args, **kwargs):
    if args and kwargs:
        raise TypeError('jsonify() behavior undefined when passed both args and kwargs')
    elif len(args) == 1:  # single args are passed directly to dumps()
        data = args[0]
    else:
        data = args or kwargs

    # turn this to False to be even faster, but warning then responses may not cache
    sort_keys = True

    return current_app.response_class(
        dumps(data,
              skipkeys=True,
              ensure_ascii=False,
              check_circular=False,
              allow_nan=True,
              cls=None,
              indent=None,
              # separators=None,
              default=None,
              sort_keys=sort_keys) + '\n', mimetype=current_app.config['JSONIFY_MIMETYPE']
    )

def find_normalized_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    # the lookup order matters
    # assumes no spaces, no dashes, and all lowercase
    # inspired by https://github.com/CottageLabs/blackbox/blob/fc13e5855bd13137cf1ef8f5e93883234fdab464/service/licences.py
    # thanks CottageLabs!  :)

    license_lookups = [
        ("koreanjpathol.org/authors/access.php", "cc-by-nc"),  # their access page says it is all cc-by-nc now
        ("elsevier.com/openaccess/userlicense", "elsevier-specific: oa user license"),  #remove the - because is removed in normalized_text above
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "acs-specific: authorchoice/editors choice usage agreement"),

        ("creativecommons.org/licenses/byncnd", "cc-by-nc-nd"),
        ("creativecommonsattributionnoncommercialnoderiv", "cc-by-nc-nd"),
        ("ccbyncnd", "cc-by-nc-nd"),

        ("creativecommons.org/licenses/byncsa", "cc-by-nc-sa"),
        ("creativecommonsattributionnoncommercialsharealike", "cc-by-nc-sa"),
        ("ccbyncsa", "cc-by-nc-sa"),

        ("creativecommons.org/licenses/bynd", "cc-by-nd"),
        ("creativecommonsattributionnoderiv", "cc-by-nd"),
        ("ccbynd", "cc-by-nd"),

        ("creativecommons.org/licenses/bysa", "cc-by-sa"),
        ("creativecommonsattributionsharealike", "cc-by-sa"),
        ("ccbysa", "cc-by-sa"),

        ("creativecommons.org/licenses/bync", "cc-by-nc"),
        ("creativecommonsattributionnoncommercial", "cc-by-nc"),
        ("ccbync", "cc-by-nc"),

        ("creativecommons.org/licenses/by", "cc-by"),
        ("creativecommonsattribution", "cc-by"),
        ("ccby", "cc-by"),

        ("creativecommons.org/publicdomain/zero", "cc0"),
        ("creativecommonszero", "cc0"),

        ("creativecommons.org/publicdomain/mark", "pd"),
        ("publicdomain", "pd"),

        # ("openaccess", "oa")
    ]

    for (lookup, license) in license_lookups:
        if lookup in normalized_text:
            if license=="pd":
                try:
                    if "worksnotinthepublicdomain" in normalized_text.decode(errors='ignore'):
                        return None
                except:
                    # some kind of unicode exception
                    return None
            return license
    return None

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    raise TypeError(repr(o) + " is not JSON serializable")

def jsonify_fast_no_sort(*args, **kwargs):
    dumps_response = jsonify_fast_no_sort_raw(*args, **kwargs)
    return current_app.response_class(dumps_response + '\n', mimetype=current_app.config['JSONIFY_MIMETYPE'])


# from https://stackoverflow.com/a/50762571/596939
def jsonify_fast_no_sort_raw(*args, **kwargs):
    if args and kwargs:
        raise TypeError('jsonify() behavior undefined when passed both args and kwargs')
    elif len(args) == 1:  # single args are passed directly to dumps()
        data = args[0]
    else:
        data = args or kwargs

    # turn this to False to be even faster, but warning then responses may not cache
    sort_keys = False

    return dumps(data,
              skipkeys=True,
              ensure_ascii=False,
              check_circular=False,
              allow_nan=True,
              cls=None,
              default=myconverter,
              indent=None,
              # separators=None,
              sort_keys=sort_keys)


class TimingMessages(object):
    def __init__(self):
        self.start_time = time.time()
        self.section_time = time.time()
        self.messages = []

    def format_timing_message(self, message, use_start_time=False):
        my_elapsed = elapsed(self.section_time, 2)
        if use_start_time:
            my_elapsed = elapsed(self.start_time, 2)
        # now reset section time
        self.section_time = time.time()
        return "{: <30} {: >6}s".format(message, my_elapsed)

    def log_timing(self, message):
        self.messages.append(self.format_timing_message(message))

    def to_dict(self):
        self.messages.append(self.format_timing_message("TOTAL", use_start_time=True))
        return self.messages

# like the one below but similar to what we used in redshift
def normalize_title_like_sql(title):
    import re

    response = title

    if not response:
        return u""

    # just first n characters
    response = response[0:500]

    # lowercase
    response = response.lower()

    # has to be before remove_punctuation
    # the kind in titles are simple <i> etc, so this is simple
    response = re.sub(u'<.*?>', u'', response)

    # remove articles and common prepositions
    response = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", response)

    # remove everything except alphas
    response = "".join(e for e in response if (e.isalpha()))

    return response


# inspired the version above
def normalize_title(title):
    if not title:
        return ""

    # just first n characters
    response = title[0:500]

    # lowercase
    response = response.lower()

    # deal with unicode
    response = unidecode(str(response))

    # has to be before remove_punctuation
    # the kind in titles are simple <i> etc, so this is simple
    response = clean_html(response)

    # remove articles and common prepositions
    response = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", response)

    # remove everything except alphas
    response = remove_everything_but_alphas(response)

    return response

# matches sql/f_generate_inverted_index.sql python user defined function in redshift
def f_generate_inverted_index(abstract_string):
	import re
	import json
	from collections import OrderedDict

	# remove jat tags and unnecessary tags and problematic white space
	abstract_string = re.sub('\b', ' ', abstract_string)
	abstract_string = re.sub('\n', ' ', abstract_string)
	abstract_string = re.sub('\t', ' ', abstract_string)
	abstract_string = re.sub('<jats:[^<]+>', ' ', abstract_string)
	abstract_string = re.sub('</jats:[^<]+>', ' ', abstract_string)
	abstract_string = re.sub('<p>', ' ', abstract_string)
	abstract_string = re.sub('</p>', ' ', abstract_string)
	abstract_string = " ".join(re.split("\s+", abstract_string))

	# build inverted index
	invertedIndex = OrderedDict()
	words = abstract_string.split()
	for i in range(len(words)):
		if words[i] not in invertedIndex:
			invertedIndex[words[i]] = []
		invertedIndex[words[i]].append(i)
	result = {
		'IndexLength': len(words),
		'InvertedIndex': invertedIndex,
	}

	return json.dumps(result, ensure_ascii=False)


# these help us do name matching well, and identify when author names are given with lastname first
def popularity_as_chinese_lastname(word):
    # https://github.com/psychbruce/ChineseNames/blob/master/data-csv/familyname.csv
    gChineseLastName = {"gu": 4082.2490000000007, "shentu": 0.063, "zhongli": 0.021, "qiao": 1672.252, "qian": 2299.3150000000005, "ge": 1977.3660000000002, "gang": 24.098, "lian": 817.2059999999999, "liao": 4115.629999999999, "rou": 0.10400000000000001, "zong": 451.743, "sikou": 0.002, "tu": 1019.4709999999999, "seng": 0.377, "puyang": 0.304, "ti": 0.066, "te": 0.02, "ta": 20.436000000000003, "song": 8663.251, "fan": 6285.179000000002, "tuan": 0.019, "dongli": 0.011, "zisang": 0.004, "die": 0.085, "gui": 404.615, "guo": 13833.576, "gun": 0.003, "sang": 381.166, "zi": 247.70300000000003, "ze": 27.029000000000003, "chen": 54047.19599999999, "zu": 165.794, "ba": 182.44899999999996, "dian": 0.079, "diao": 366.40099999999995, "suo": 183.487, "sun": 17199.753, "cai": 5833.531, "sui": 447.13199999999995, "kuo": 0.009, "kun": 0.338, "cheng": 6478.245999999999, "dongguo": 0.004, "lei": 2928.75, "neng": 0.031, "men": 148.852, "mei": 977.3839999999999, "weichi": 0.021, "helian": 1.042, "tiao": 0.014, "qiguan": 0.051, "geng": 1287.5800000000002, "chang": 2190.452, "dantai": 0.002, "cha": 320.17600000000004, "che": 534.323, "fen": 0.009000000000000001, "chi": 670.294, "fei": 486.118, "gongjian": 0.003, "chu": 1141.8629999999996, "qun": 0.013, "zhuge": 31.967, "nong": 518.4069999999999, "ma": 15754.043000000001, "mo": 2003.9499999999998, "mi": 497.891, "mu": 899.206, "zhao": 24998.841, "zhan": 1504.8650000000002, "cao": 7389.7210000000005, "can": 0.253, "ning": 985.974, "wang": 92955.796, "beng": 0.001, "zhuang": 1248.091, "tan": 6915.151, "tao": 2602.436, "tai": 161.58700000000002, "zhang": 84549.61, "ping": 165.282, "huangfu": 49.179, "hou": 3725.4020000000005, "lan": 2054.4680000000003, "lao": 230.203, "lai": 2011.609, "zhuansun": 0.001, "fa": 15.452, "mai": 457.695, "xiang": 2808.8940000000002, "mao": 2634.41, "man": 207.114, "a": 172.58700000000002, "jiang": 13904.997999999998, "kuang": 710.417, "bing": 36.445, "su": 5631.705, "si": 627.1980000000001, "sa": 31.166999999999998, "se": 0.001, "ding": 5333.702, "xuan": 237.33700000000002, "kong": 2128.315, "pang": 1614.1879999999999, "jie": 746.7740000000001, "jia": 4166.276, "jin": 5363.212000000001, "linghu": 43.798, "li": 90150.00300000003, "lv": 5356.0199999999995, "lu": 12322.706, "yi": 1960.0150000000003, "ya": 35.958, "cen": 423.622, "dan": 1033.586, "dao": 107.199, "ye": 5516.030000000001, "dai": 4086.3949999999995, "zhen": 324.25, "bang": 0.178, "yu": 14820.900000000003, "ouyang": 807.92, "en": 4.405, "kang": 2038.143, "xianyu": 1.218, "er": 7.009, "ru": 145.37099999999998, "keng": 0.001, "fu": 6138.212, "re": 0.059, "ren": 5122.602, "gou": 507.89700000000005, "ri": 0.077, "tian": 6420.280999999999, "qi": 3246.143000000001, "que": 123.888, "bao": 1961.003, "shun": 3.187, "shuo": 0.077, "shui": 118.906, "xue": 3167.084, "yun": 146.479, "xun": 127.49199999999999, "changsun": 0.024, "yue": 1420.9080000000001, "gongliang": 0.013, "zan": 83.454, "zao": 0.07600000000000001, "rang": 0.097, "xi": 807.4060000000001, "yong": 176.617, "zai": 0.095, "guan": 2292.234000000001, "guai": 0.006, "xuanyuan": 0.021, "dong": 6371.588, "kuai": 37.329, "ying": 437.284, "kuan": 0.105, "xu": 25572.703, "xia": 3938.641, "xie": 8824.359, "yin": 4870.4220000000005, "rong": 575.471, "xin": 967.0949999999999, "nian": 82.65, "niao": 0.001, "xiu": 139.586, "fo": 0.007, "kou": 356.82499999999993, "hua": 968.491, "hun": 0.003, "huo": 926.5, "hui": 313.06499999999994, "quan": 570.0920000000001, "shuai": 176.242, "chong": 21.497, "bei": 53.763, "ben": 0.14600000000000002, "zongzheng": 0.366, "zishu": 0.006, "dang": 477.03000000000003, "sai": 26.056, "ang": 25.663, "san": 0.145, "ran": 895.891, "rao": 662.075, "ming": 323.703, "wansi": 0.003, "lie": 0.106, "min": 395.31899999999996, "pa": 0.008, "lin": 11822.310000000001, "mian": 0.019, "mie": 0.11099999999999999, "liu": 63884.39, "zou": 3616.674, "miu": 0.001, "kai": 24.189000000000004, "kao": 0.076, "kan": 184.731, "ka": 0.004, "ke": 867.87, "zuoqiu": 0.008, "sikong": 0.012, "yang": 40605.179000000004, "ku": 0.376, "nangong": 0.045, "deng": 7374.0470000000005, "lezheng": 1.833, "dou": 673.0589999999999, "shou": 44.321, "liangqiu": 0.019, "wenren": 0.142, "chuang": 0.017, "feng": 8063.331, "meng": 4280.351, "gongzu": 0.048, "kui": 0.029000000000000005, "di": 1655.5459999999996, "de": 41.641, "da": 113.428, "du": 5869.716000000001, "gen": 0.001, "qu": 1855.41, "shu": 1080.463, "nanrong": 1.065, "sha": 404.839, "she": 308.78900000000004, "ban": 245.552, "shi": 9673.819000000001, "xiahou": 0.214, "bai": 4052.0980000000004, "shangguan": 61.554, "nuo": 63.885999999999996, "sen": 0.135, "fang": 4430.121000000001, "teng": 756.2339999999999, "daxi": 0.001, "lun": 38.479, "luo": 12616.87, "wa": 0.198, "wo": 0.321, "ju": 911.7040000000001, "wu": 28215.747000000007, "le": 261.88399999999996, "ji": 3416.4739999999997, "huang": 27775.41, "tuo": 93.75900000000001, "la": 83.74200000000002, "mang": 0.047, "dongfang": 0.942, "ci": 0.40399999999999997, "tong": 1575.034, "ce": 0.015, "na": 128.891, "qin": 3542.044, "cu": 0.001, "peng": 6983.668000000001, "dun": 33.445, "duo": 68.08700000000002, "gongshan": 0.008, "ting": 1.5289999999999997, "qie": 0.012, "yao": 5085.334, "jiu": 11.134, "guliang": 0.067, "pi": 213.733, "ceng": 6680.022000000001, "chun": 21.721000000000004, "qia": 0.1, "chui": 0.008, "gao": 12506.908, "gan": 1222.721, "zeng": 0.004, "gai": 177.367, "xiong": 3563.77, "gongzhong": 0.024, "tang": 10500.794999999998, "pian": 0.08199999999999999, "piao": 0.136, "cang": 70.625, "heng": 79.049, "xian": 385.146, "xiao": 6631.245999999999, "bian": 805.962, "biao": 0.046, "duan": 2976.8920000000003, "zhongsun": 0.012, "cong": 375.20599999999996, "situ": 33.645, "zhuo": 517.594, "hong": 1850.938, "shuang": 34.201, "qidiao": 0.005, "juan": 1.3519999999999999, "pai": 0.002, "shan": 72.313, "shao": 2434.11, "pan": 6292.063999999999, "pao": 0.021, "hang": 118.788, "nie": 1469.755, "zhuan": 0.024, "yuan": 6836.450999999998, "niu": 1954.955, "gongye": 0.005, "gongyi": 0.001, "gongyu": 1.14, "miao": 933.871, "guang": 16.909, "gongyang": 0.006, "gongbo": 0.03, "sima": 14.421, "hai": 193.602, "han": 7593.701, "hao": 2485.772, "wei": 10146.847000000003, "wen": 4000.579, "ruan": 944.949, "cuo": 0.176, "cun": 0.263, "gongsun": 0.073, "cui": 4757.728, "bin": 93.738, "gongshang": 0.003, "bie": 55.993, "mou": 1176.143, "murong": 0.423, "ximen": 0.007, "nv": 0.002, "weng": 788.365, "shen": 6371.549000000001, "xing": 1865.3760000000002, "qiang": 183.785, "nuan": 0.019, "pen": 0.008, "pei": 931.798, "rui": 139.04600000000002, "run": 0.048, "ruo": 0.041, "sheng": 972.629, "dui": 0.004, "bo": 134.90900000000002, "bi": 1115.1449999999998, "bu": 610.713, "huyan": 5.128, "chuan": 0.107, "qing": 240.513, "chuai": 0.087, "pu": 1289.63, "chou": 372.14599999999996, "ou": 1266.7169999999999, "ziche": 0.001, "luan": 376.815, "zuo": 1238.5559999999998, "duangan": 0.147, "jian": 478.58699999999993, "jiao": 1300.38, "diwu": 0.014, "wan": 2422.3320000000003, "jing": 1182.6859999999995, "qiong": 0.019, "wai": 0.002, "long": 2741.2650000000003, "yan": 8148.3150000000005, "liang": 9949.785999999998, "lou": 697.458, "huan": 27.355, "hei": 57.075, "huai": 26.305, "jue": 0.003, "shang": 1388.719, "jun": 4.077000000000001, "hu": 14510.728000000003, "ling": 945.9079999999999, "ha": 71.353, "he": 15742.324000000002, "zhu": 16911.276, "po": 165.47400000000002, "zha": 85.889, "zhe": 0.52, "zhi": 340.8359999999999, "gong": 3464.8500000000004, "ai": 655.784, "chai": 800.687, "chan": 0.087, "chao": 179.552, "ao": 293.00600000000003, "an": 1731.2469999999998, "qiu": 3698.122, "ni": 1550.8659999999998, "duanmu": 0.048, "nanmen": 0.028, "gongxi": 0.029, "zhong": 5041.214999999999, "zang": 401.545, "nai": 0.056, "nan": 262.435, "nao": 0.083, "chuo": 0.014, "tie": 64.625, "you": 1413.0349999999996, "chushi": 0.009, "zheng": 10775.93, "leng": 466.976, "zun": 0.001, "zhou": 23192.945000000003, "zhongchang": 0.74, "yuwen": 9.416, "lang": 347.352, "e": 64.08000000000001, "wuma": 0.031}
    return float(gChineseLastName.get(word.lower(), '0.0'))

def popularity_as_japanese_lastname(word):
    # https://github.com/jackdeguest/JapaneseNamesDatabase
    gJapaneseLastName = {"sato": 1905.3, "suzuki": 1809.0, "takahashi": 1425.0, "tanaka": 1355.2, "ito": 1202.9, "watanabe": 1324.6, "yamamoto": 1084.6, "nakamura": 1082.5, "kobayashi": 1043.3, "kato": 896.9, "yoshida": 839.2, "yamada": 820.0, "sasaki": 691.3, "yamaguchi": 649.0, "matsumoto": 657.7, "inoue": 621.6, "kimura": 581.0, "hayashi": 550.0, "saito": 1002.2, "shimizu": 550.3, "yamazaki": 488.9, "mori": 469.0, "ikeda": 454.0, "hashimoto": 455.6, "abe": 541.0, "ishikawa": 434.2, "yamashita": 423.0, "nakajima": 485.59999999999997, "ogawa": 412.90000000000003, "ishii": 398.0, "maeda": 385.0, "okada": 382.0, "hasegawa": 380.0, "fujita": 378.0, "goto": 396.59999999999997, "kondou": 372.0, "murakami": 358.0, "endou": 336.0, "aoki": 331.0, "sakamoto": 390.1, "fukuda": 315.0, "ota": 357.9, "nishimura": 311.0, "fujii": 314.5, "fujiwara": 299.0, "okamoto": 307.1, "miura": 298.0, "kaneko": 308.5, "nakano": 313.5, "nakagawa": 304.2, "harada": 293.0, "matsuda": 291.0, "takeuchi": 322.1, "ono": 511.6, "tamura": 282.0, "nakayama": 278.2, "wada": 269.0, "ishida": 268.0, "morita": 281.5, "ueda": 336.3, "hara": 247.0, "uchida": 245.0, "shibata": 259.4, "sakai": 372.9, "miyazaki": 239.0, "yokoyama": 238.0, "takagi": 246.6, "andou": 232.0, "miyamoto": 234.9, "kojima": 283.20000000000005, "kudou": 217.0, "taniguchi": 217.0, "imai": 214.0, "takada": 211.0, "maruyama": 213.2, "masuda": 248.2, "sugiyama": 210.5, "murata": 207.0, "otsuka": 206.0, "oyama": 307.59999999999997, "fujimoto": 211.3, "heiya": 204.0, "arai": 328.0, "kono": 205.4, "ueno": 216.7, "takeda": 292.0, "noguchi": 200.0, "matsui": 199.2, "chiba": 195.0, "sugawara": 194.0, "iwasaki": 193.0, "kubo": 193.6, "kinoshita": 189.0, "sano": 187.0, "nomura": 186.0, "matsuo": 185.0, "kikuchi": 347.0, "sugimoto": 183.0, "ichikawa": 182.0, "furukawa": 178.0, "onishi": 178.0, "shimada": 231.6, "mizuno": 175.0, "sakurai": 197.8, "takano": 179.1, "yoshikawa": 173.9, "yamauchi": 165.0, "nishida": 164.0, "iida": 162.0, "nishikawa": 165.0, "komatsu": 161.0, "kitamura": 163.8, "yasuda": 174.9, "igarashi": 158.0, "kawaguchi": 156.0, "hirata": 155.0, "kan": 215.2, "nakada": 180.9, "kubota": 205.6, "higashi": 152.0, "hattori": 151.0, "kawasaki": 167.6, "iwata": 150.0, "tsuchiya": 168.6, "fukushima": 163.2, "honda": 226.3, "tsuji": 149.0, "higuchi": 148.0, "taguchi": 147.0, "nagai": 183.6, "akiyama": 149.3, "sanchu": 147.0, "nakanishi": 153.0, "yoshimura": 149.4, "kawakami": 162.2, "ohashi": 142.0, "ishihara": 142.0, "matsuoka": 142.0, "hamada": 167.0, "baba": 142.0, "morimoto": 150.6, "yano": 139.0, "asano": 144.0, "matsushita": 138.0, "hoshino": 138.0, "okubo": 146.1, "yoshioka": 137.0, "koike": 137.0, "noda": 135.0, "araki": 138.1, "kumagaya": 132.0, "matsuura": 132.0, "otani": 134.4, "naito": 131.0, "kuroda": 129.0, "ozaki": 142.2, "kawamura": 212.4, "nagata": 208.6, "mochizuki": 126.0, "hori": 125.0, "matsumura": 125.0, "tanabe": 137.8, "kanno": 154.70000000000002, "hirai": 124.0, "oshima": 147.2, "nishiyama": 124.0, "hayakawa": 124.0, "kurihara": 123.0, "hirose": 142.9, "yokota": 119.0, "ishibashi": 118.0, "iwamoto": 131.1, "katayama": 118.0, "hagiwara": 117.0, "sekiguchi": 115.0, "miyata": 115.0, "oishi": 114.0, "homma": 114.0, "sudou": 113.0, "kozan": 113.0, "okazaki": 112.0, "oda": 155.3, "yoshino": 119.5, "kamada": 111.0, "shinohara": 110.0, "uehara": 114.8, "konishi": 111.3, "matsubara": 108.0, "fukui": 108.0, "narita": 108.0, "koga": 118.3, "omori": 107.0, "minami": 107.0, "koizumi": 108.5, "okumura": 106.0, "uchiyama": 106.0, "sawada": 150.1, "kuwahara": 105.0, "miyake": 105.0, "kataoka": 105.0, "kawashima": 135.6, "oka": 104.0, "tomita": 151.4, "okuda": 103.0, "yagi": 109.4, "sugiura": 103.0, "matsunaga": 105.8, "ozawa": 155.8, "kitagawa": 104.4, "hirayama": 101.0, "kawai": 184.5, "makino": 104.1, "sekine": 101.0, "shiroishi": 100.0, "imamura": 99.0, "terada": 98.7, "aoyama": 98.3, "nakao": 100.7, "ogura": 122.0, "kamimura": 102.6, "onodera": 96.5, "shibuya": 101.1, "okamura": 95.3, "sakaguchi": 116.5, "adachi": 178.50000000000003, "tada": 94.3, "amano": 94.3, "kompon": 93.1, "sakuma": 95.5, "toyoda": 92.3, "murayama": 92.0, "kakuda": 91.8, "iizuka": 91.7, "tajima": 109.39999999999999, "nishi": 91.5, "muto": 91.1, "miyashita": 90.7, "shirai": 90.5, "kodama": 112.69999999999999, "tsukamoto": 90.0, "sakata": 102.6, "mizutani": 89.4, "enomoto": 89.2, "kamiya": 88.9, "kohara": 87.4, "ogasawara": 87.2, "morishita": 86.7, "asai": 89.6, "okabe": 86.5, "nakai": 102.89999999999999, "kanda": 93.89999999999999, "maekawa": 84.8, "miyagawa": 84.8, "inagaki": 84.4, "okawa": 84.1, "matsuzaki": 82.6, "wakabayashi": 82.3, "iijima": 86.4, "tani": 81.5, "osawa": 117.80000000000001, "ishizuka": 80.6, "oikawa": 86.0, "horiuchi": 80.0, "tashiro": 79.1, "yamane": 78.9, "eguchi": 78.7, "nakatani": 86.5, "kishimoto": 77.7, "arakawa": 77.4, "nishio": 77.3, "moriyama": 81.7, "konno": 121.80000000000001, "hosokawa": 76.6, "okano": 76.2, "kanai": 76.0, "toda": 75.6, "inaba": 79.30000000000001, "tsuda": 75.3, "morikawa": 75.1, "tsuchii": 104.8, "hoshi": 74.9, "muramatsu": 74.7, "ochiai": 74.6, "hatakeyama": 89.3, "mikami": 80.5, "machida": 73.6, "nagao": 83.7, "iwai": 72.7, "nakahara": 76.6, "tsutsumi": 72.3, "nozaki": 70.9, "nakazawa": 103.2, "yoneda": 70.3, "kaneda": 78.5, "matsuyama": 70.0, "hotta": 69.5, "miyoshi": 69.4, "sugita": 69.1, "nishino": 69.0, "saeki": 68.8, "yamagishi": 68.6, "nishioka": 67.8, "kurokawa": 70.5, "izumi": 100.10000000000001, "kai": 69.60000000000001, "otake": 74.9, "kuroki": 65.7, "kasahara": 65.7, "tokunaga": 65.4, "horie": 65.3, "kawada": 65.2, "taiboku": 65.1, "suda": 65.1, "kishi": 68.3, "yamakawa": 64.9, "furuta": 64.7, "nitta": 64.5, "umeda": 64.3, "miki": 64.2, "nonaka": 64.0, "sakakibara": 63.7, "murai": 63.0, "okuyama": 62.9, "tsuchida": 62.7, "takizawa": 74.0, "omura": 61.8, "oshiro": 61.7, "kawabata": 105.7, "kinjou": 61.1, "inokuchi": 60.5, "kajiwara": 60.4, "oba": 90.80000000000001, "yoshihara": 58.4, "kanazawa": 78.7, "miyagi": 58.2, "miyauchi": 58.0, "nagashima": 105.8, "yasui": 60.0, "shouji": 98.9, "ouchi": 57.4, "higa": 57.0, "hidaka": 57.0, "motegi": 59.199999999999996, "mukai": 56.8, "matsushima": 64.2, "nishimoto": 61.8, "shimoda": 61.1, "ogino": 56.4, "tsukada": 56.2, "takenaka": 56.2, "okuno": 56.1, "ishiguro": 56.1, "hirota": 74.2, "fujikawa": 55.8, "fukumoto": 70.4, "kurita": 55.6, "kitano": 55.3, "kawara": 98.19999999999999, "uno": 58.7, "tanigawa": 55.0, "kotani": 54.9, "fujino": 54.8, "yoshimoto": 62.5, "tamba": 58.5, "fujioka": 54.5, "takemoto": 66.7, "takeshita": 56.9, "ogata": 98.80000000000001, "aoyagi": 54.1, "fujimura": 53.9, "furuya": 108.4, "hirakawa": 53.6, "kamei": 53.6, "takashima": 73.5, "miwa": 53.3, "fujisawa": 67.9, "shinozaki": 52.3, "miyahara": 52.3, "takai": 52.0, "shimomura": 51.8, "takase": 51.5, "sanson": 51.4, "kawamoto": 91.7, "negishi": 51.3, "yanagisawa": 65.3, "deguchi": 51.0, "yokoi": 50.7, "komori": 53.300000000000004, "yoshizawa": 73.9, "takei": 69.5, "nagano": 97.9, "takemura": 58.1, "miyazawa": 67.6, "hiramatsu": 49.2, "fukuoka": 49.1, "kurosawa": 66.8, "mizoguchi": 48.8, "usui": 74.89999999999999, "tahara": 48.7, "shimura": 48.7, "ineda": 48.6, "asada": 57.5, "tsutsui": 48.0, "yanagida": 47.8, "ohara": 51.1, "hayashida": 47.7, "ohira": 47.7, "fukunaga": 47.2, "seto": 47.2, "tezuka": 47.1, "irie": 47.1, "kitahara": 47.0, "tominaga": 65.9, "shinoda": 55.8, "tsuruta": 46.6, "yuasa": 46.6, "koide": 46.4, "nagaoka": 56.8, "numata": 46.3, "takamatsu": 46.3, "yajima": 53.1, "yamaoka": 46.1, "horiguchi": 45.9, "iwase": 45.5, "ishizaki": 45.4, "otsuki": 55.3, "ishiyama": 45.3, "soma": 44.8, "horikawa": 44.8, "ikegami": 44.7, "ninomiya": 44.7, "sonoda": 49.5, "hiraoka": 44.3, "kashiwagi": 44.2, "hanada": 44.1, "shimazaki": 55.7, "sugihara": 43.8, "kawano": 43.7, "kanou": 46.6, "murase": 43.4, "katagiri": 43.4, "nagasawa": 70.9, "ochi": 43.3, "kurata": 47.4, "utsumi": 43.1, "nozawa": 56.3, "fukuhara": 42.9, "nishihara": 42.8, "matsuno": 42.8, "kasai": 92.2, "akimoto": 61.8, "kosaka": 47.1, "tahata": 42.1, "tanimoto": 41.9, "hino": 44.699999999999996, "chida": 41.8, "kitajima": 48.3, "yoshii": 41.5, "fukazawa": 56.599999999999994, "nishizawa": 56.0, "tokuda": 41.1, "aizawa": 62.9, "shintani": 40.7, "haraguchi": 40.7, "tagami": 40.7, "oyanagi": 44.800000000000004, "yoneyama": 40.4, "hosoya": 45.0, "morioka": 40.2, "imaizumi": 40.2, "hatanaka": 59.2, "hamano": 45.3, "haga": 49.5, "oi": 39.5, "akita": 42.9, "isobe": 48.0, "osaki": 39.1, "shirakawa": 39.1, "ueki": 43.0, "tsuboi": 41.199999999999996, "nakamoto": 47.599999999999994, "mitani": 38.8, "aihara": 44.4, "hosoda": 38.8, "minagawa": 38.7, "ogiwara": 38.7, "asami": 43.800000000000004, "hamaguchi": 44.9, "fukuyama": 38.6, "kishida": 38.6, "tsujimoto": 38.4, "kawase": 44.900000000000006, "kihara": 38.2, "otomo": 38.2, "hatake": 44.400000000000006, "hirabayashi": 38.0, "kawauchi": 52.7, "shioda": 37.9, "toyama": 106.69999999999999, "shimamura": 47.5, "otsubo": 37.6, "mitsui": 41.0, "uemura": 37.5, "namba": 45.7, "kamiyama": 37.4, "nakazato": 45.1, "mimura": 37.2, "mitsuhashi": 37.1, "iwashita": 37.1, "kuriyama": 37.0, "ide": 73.3, "matsukawa": 36.9, "yoshinaga": 36.8, "hayasaka": 36.7, "satake": 36.6, "kawata": 36.2, "kameyama": 36.1, "hamasaki": 42.0, "tachibana": 67.3, "asakura": 43.3, "yanagi": 36.0, "kusano": 35.9, "horii": 35.5, "hosaka": 38.599999999999994, "shiga": 35.2, "muraoka": 35.2, "takamura": 35.1, "handa": 35.0, "kano": 53.3, "wakamatsu": 35.0, "suijou": 35.0, "komiya": 39.0, "otsu": 34.8, "takaoka": 34.8, "nakane": 34.7, "kameda": 34.7, "takayanagi": 34.7, "tateishi": 34.7, "shibasaki": 42.2, "ichimura": 34.4, "mishima": 42.1, "segawa": 34.4, "terashima": 44.3, "nara": 34.3, "seino": 34.2, "manabe": 39.900000000000006, "taira": 60.1, "tamai": 34.0, "mizuguchi": 33.9, "niigaki": 33.9, "naiya": 33.9, "fujisaki": 33.8, "uematsu": 40.599999999999994, "kiuchi": 33.8, "onuma": 33.7, "eto": 65.0, "shindou": 55.7, "kuno": 33.7, "hashiguchi": 33.7, "takami": 33.6, "fukaya": 33.6, "teramoto": 35.8, "tamaki": 81.0, "miyaji": 38.099999999999994, "moriya": 46.7, "fujimori": 33.1, "sugaya": 33.0, "mouri": 33.0, "oe": 32.8, "miyajima": 44.5, "namiki": 35.5, "arima": 32.8, "nasu": 37.400000000000006, "gezan": 32.7, "arida": 32.7, "kogen": 32.6, "ichihara": 32.5, "yabe": 32.4, "iino": 32.1, "torii": 41.7, "kitayama": 32.1, "sakashita": 34.5, "ishiwatari": 32.0, "sekiya": 39.099999999999994, "matsuzawa": 44.6, "itagaki": 32.0, "nogami": 31.8, "maehara": 31.6, "mihara": 31.6, "maki": 45.2, "sakaue": 31.5, "tazaki": 31.2, "hirao": 31.1, "matsuki": 31.1, "iwasa": 31.1, "takao": 30.8, "naruse": 30.7, "hase": 30.7, "komuro": 30.7, "mita": 30.6, "someya": 30.6, "takimoto": 35.300000000000004, "nagase": 45.7, "usami": 39.099999999999994, "nomoto": 37.4, "kodera": 30.2, "tsukahara": 30.2, "itakura": 30.2, "takasaki": 30.2, "akiba": 38.800000000000004, "yamashiro": 30.1, "kadowaki": 30.1, "takiguchi": 33.2, "ishigaki": 30.0, "yamano": 29.9, "hasebe": 29.9, "umino": 29.9, "ida": 34.6, "okura": 34.8, "shin": 41.3, "ezaki": 29.7, "masaki": 38.300000000000004, "imada": 29.5, "takanashi": 29.5, "shiina": 29.3, "aikawa": 29.3, "oku": 29.1, "hiratsuka": 29.1, "ozeki": 63.8, "shima": 43.1, "shishido": 29.0, "iwanaga": 28.9, "fujiki": 28.9, "tsuzuki": 33.699999999999996, "nishitani": 28.8, "kida": 34.599999999999994, "hosoi": 28.7, "egawa": 28.7, "kanaya": 28.7, "kanayama": 28.6, "horikoshi": 28.6, "kosugi": 28.6, "ishimaru": 28.6, "inui": 28.5, "tagawa": 28.5, "nagayama": 44.4, "suwa": 28.5, "kanamori": 28.4, "moritani": 28.4, "kuroiwa": 28.3, "shimokawa": 28.2, "togashi": 33.0, "obata": 64.89999999999999, "hosono": 28.1, "sugimura": 28.0, "fuse": 27.9, "umehara": 27.9, "nohara": 27.9, "ueyama": 32.6, "yamagami": 27.8, "onuki": 36.0, "kitada": 27.8, "ishimoto": 30.3, "yonezawa": 37.5, "okuma": 30.8, "toyoshima": 33.9, "onoda": 27.8, "miyawaki": 27.7, "akamatsu": 27.7, "suenaga": 27.7, "akutsu": 31.400000000000002, "oya": 52.9, "furusawa": 39.8, "miyano": 27.4, "shioya": 30.0, "imanishi": 27.2, "kuramochi": 27.2, "suiden": 27.1, "shimabukuro": 27.0, "kazama": 27.0, "aso": 30.7, "motohashi": 26.9, "kagawa": 35.4, "sugenuma": 26.8, "ikuta": 26.8, "kadota": 26.8, "kita": 58.0, "ikawa": 26.6, "nishiguchi": 26.5, "owada": 26.4, "nakabayashi": 26.3, "kokubo": 28.3, "umemoto": 26.2, "iwabuchi": 35.400000000000006, "suga": 26.1, "urata": 26.1, "tomioka": 34.8, "sagawa": 26.1, "hamamoto": 30.6, "iwama": 26.0, "sugino": 25.9, "otaki": 36.4, "yaguchi": 25.9, "dobashi": 25.9, "kusaka": 25.8, "kimoto": 35.6, "kawabe": 41.7, "kido": 46.1, "ebihara": 30.9, "hatano": 49.1, "kanemaru": 25.5, "nagasaki": 25.5, "haneda": 29.4, "sunagawa": 25.4, "meguro": 25.2, "hirasawa": 34.3, "moriwaki": 25.2, "tachikawa": 27.5, "konuma": 25.2, "amemiya": 25.2, "sanada": 25.1, "asakawa": 25.1, "hakucho": 25.1, "yoda": 27.5, "yamagata": 37.300000000000004, "fukada": 25.0, "itabashi": 25.0, "kanzaki": 25.0, "ishizaka": 25.0, "kageyama": 45.6, "ohata": 32.9, "sone": 34.099999999999994, "terao": 24.9, "yada": 24.8, "kitazawa": 32.9, "takakura": 24.4, "saegusa": 24.4, "katsumata": 41.699999999999996, "akagi": 29.0, "hashizume": 37.5, "umezu": 24.2, "kiyoda": 24.2, "udagawa": 24.2, "tanno": 24.1, "utsunomiya": 24.1, "yanagawa": 27.1, "ashida": 24.0, "onoe": 24.0, "seo": 23.9, "iwaki": 30.599999999999998, "yokoo": 23.7, "yanagihara": 23.6, "ikeuchi": 23.6, "tsuruoka": 23.6, "ebara": 23.6, "sakano": 27.7, "kuribayashi": 23.5, "nishiwaki": 23.5, "kuwata": 23.5, "kume": 26.4, "kaku": 31.299999999999997, "yuuki": 23.4, "kawanishi": 23.3, "hibino": 23.3, "moriguchi": 23.3, "nishijima": 32.5, "umemura": 23.2, "kobori": 23.2, "uesugi": 23.2, "tejima": 32.900000000000006, "hida": 37.7, "ihara": 34.2, "urano": 23.1, "kosuge": 23.1, "shuto": 23.0, "akasaka": 23.0, "tabata": 22.9, "aono": 22.9, "kashiwabara": 22.8, "yokokawa": 22.7, "obayashi": 25.200000000000003, "odaka": 47.1, "gouda": 26.2, "asanuma": 22.5, "mizushima": 27.9, "umesawa": 31.8, "takehara": 22.3, "kakutani": 22.2, "miyasaka": 22.2, "wakita": 22.1, "ariga": 24.400000000000002, "shida": 25.8, "ikemoto": 22.1, "misawa": 30.9, "motomura": 25.8, "shibayama": 25.2, "shimazu": 28.2, "akashi": 25.0, "imoo": 21.8, "iketani": 21.7, "kasuga": 21.7, "oshita": 21.7, "katsuta": 21.6, "takikawa": 24.8, "yamato": 25.200000000000003, "miyazato": 21.5, "kusunoki": 26.4, "kase": 21.5, "nakatsuka": 21.4, "maejima": 25.299999999999997, "aida": 39.6, "soga": 28.0, "totsuka": 21.2, "onda": 21.1, "kakinuma": 21.1, "sasagawa": 21.0, "kawagoe": 21.0, "maeno": 21.0, "tanimura": 21.0, "oguri": 21.0, "kusumoto": 23.9, "ishino": 21.0, "kokubun": 21.0, "shiraki": 20.9, "yoshizaki": 20.9, "bandou": 28.799999999999997, "furuyama": 20.9, "isono": 24.2, "sugai": 34.0, "natsume": 20.8, "nojima": 23.400000000000002, "okayama": 20.7, "kusakabe": 20.7, "imoto": 26.9, "uchimura": 20.7, "hanaoka": 20.6, "takahata": 31.200000000000003, "kumada": 20.5, "ishikura": 20.5, "tabuchi": 31.6, "naka": 39.0, "oki": 20.5, "hama": 24.5, "kobe": 20.3, "fukuchi": 20.3, "taki": 24.0, "kuramoto": 26.9, "mochida": 20.3, "suekichi": 20.3, "tadokoro": 20.2, "wakayama": 20.2, "shiomi": 20.2, "okita": 26.9, "kito": 27.7, "terasawa": 26.5, "kurosaki": 20.1, "hatta": 23.9, "morii": 20.0, "sawai": 28.2, "morinaga": 20.0, "shigematsu": 20.0, "oura": 19.9, "koya": 32.2, "honzan": 19.9, "teranishi": 19.9, "muraki": 19.9, "ima": 19.8, "imura": 19.8, "tajiri": 19.8, "shigeta": 29.2, "kasuya": 19.7, "nagasaka": 23.099999999999998, "fukai": 19.7, "nambu": 19.6, "hiraga": 19.5, "tamada": 19.5, "kikuta": 19.5, "ukai": 19.4, "yonekura": 19.4, "isozaki": 19.3, "teraoka": 19.2, "iwatani": 19.2, "okajima": 24.299999999999997, "inomata": 39.099999999999994, "sasahara": 19.2, "kumakura": 19.2, "sakurada": 21.700000000000003, "noro": 19.0, "masui": 23.2, "ujiie": 19.0, "chinen": 18.9, "furuhashi": 18.9, "koguchi": 26.500000000000004, "tomizawa": 32.6, "gomi": 18.8, "kodaira": 18.8, "omata": 21.4, "nagahama": 24.1, "kawazoe": 20.9, "kogure": 28.400000000000002, "nishii": 18.6, "makita": 28.099999999999998, "nishiura": 18.5, "niimura": 18.5, "kajita": 18.5, "yasunaga": 18.5, "iwamura": 18.4, "yanase": 25.9, "shitaji": 18.4, "maruta": 18.4, "hiramoto": 18.4, "kurahashi": 18.3, "hanai": 18.3, "okoshi": 20.400000000000002, "imagawa": 18.3, "tsujimura": 18.3, "kashima": 28.2, "naganuma": 24.5, "sugisaki": 18.2, "fujishima": 22.200000000000003, "matsutani": 18.1, "momose": 18.0, "terasaki": 18.0, "nihei": 18.0, "kamihara": 18.0, "tsukagoshi": 18.0, "komine": 26.3, "terai": 18.0, "han": 18.0, "minato": 18.0, "terauchi": 18.0, "wagatsuma": 18.0, "yasukawa": 17.9, "shinbo": 17.9, "akabane": 20.5, "furuichi": 17.8, "kawana": 17.8, "date": 17.8, "mashiko": 34.099999999999994, "hongou": 17.6, "ura": 17.6, "shinagawa": 17.6, "higashino": 17.5, "takita": 20.3, "shinozuka": 17.5, "omae": 17.5, "muroi": 17.5, "noma": 17.5, "mano": 22.299999999999997, "ishizawa": 24.0, "toi": 21.599999999999998, "kumazawa": 23.7, "matoba": 17.4, "gunji": 20.6, "nakaoka": 17.3, "heigen": 17.3, "kin": 22.0, "eda": 20.3, "katano": 17.3, "sunaga": 19.8, "murakoshi": 17.2, "muranaka": 17.2, "shinkawa": 17.2, "jinbo": 17.2, "omiya": 17.1, "terui": 17.1, "fujiyama": 17.1, "nagumo": 17.0, "nagatomo": 21.3, "atsumi": 17.0, "kanehara": 17.0, "anzai": 38.5, "saka": 16.9, "nojiri": 16.9, "oiwa": 16.9, "yukawa": 16.9, "sumita": 16.8, "hisada": 16.8, "akai": 16.8, "yasuhara": 16.8, "morishima": 20.1, "kurimoto": 16.8, "hirokawa": 20.099999999999998, "takasu": 16.7, "kako": 23.5, "matsuhashi": 16.6, "ogawara": 23.3, "yabuki": 16.6, "masuyama": 16.6, "kitaoka": 16.6, "ishioka": 16.6, "tsumura": 16.6, "oyamada": 16.6, "shiozaki": 16.6, "kaji": 34.7, "fushimi": 16.6, "funakoshi": 19.0, "arimura": 16.5, "tsubota": 16.5, "kamioka": 16.5, "sagara": 16.5, "yoshimi": 16.5, "takatsu": 16.5, "ishimura": 16.4, "fujitani": 16.4, "yazaki": 16.3, "nakamori": 16.3, "nishimori": 16.3, "sekimoto": 16.3, "isogai": 16.3, "fukushi": 16.3, "yamaki": 25.799999999999997, "tanita": 16.2, "anto": 16.2, "kuwano": 16.2, "midorikawa": 16.2, "fujimaki": 16.2, "takasaka": 16.2, "kakiuchi": 16.1, "yamawaki": 16.1, "kajiyama": 16.1, "inamura": 16.1, "takazawa": 22.400000000000002, "sekino": 16.0, "morisaki": 16.0, "hamanaka": 18.3, "mizumoto": 20.2, "komura": 21.4, "chudou": 16.0, "nakasone": 18.5, "tanahashi": 15.9, "taniuchi": 15.9, "yumoto": 15.9, "matsubayashi": 15.8, "nimura": 15.8, "sumiyoshi": 15.8, "ushijima": 18.6, "sakoda": 15.8, "sakaki": 18.0, "komiyama": 25.5, "kotake": 15.6, "teruya": 15.6, "funaki": 21.5, "kaga": 15.6, "tamagawa": 15.6, "hitomi": 15.5, "kakizaki": 15.5, "higashiyama": 15.5, "iimura": 15.5, "kakimoto": 18.5, "tsubaki": 15.4, "shiozawa": 20.2, "shiono": 15.4, "osaka": 15.4, "akatsuka": 15.4, "niimi": 23.5, "tsurumi": 15.4, "umeki": 15.3, "maruoka": 15.3, "yoshitake": 18.599999999999998, "koshikawa": 15.2, "tazawa": 20.6, "takamori": 15.1, "yazawa": 21.2, "shimotsuke": 15.1, "kiyohara": 15.0, "karasawa": 23.8, "mukaiyama": 15.0, "kobashi": 15.0, "komai": 15.0, "sawa": 23.4, "isawa": 42.0, "takasugi": 14.9, "uta": 14.9, "same-shima": 14.8, "tsujii": 14.8, "sambe": 14.8, "chikurin": 14.8, "hirama": 14.8, "takabayashi": 14.8, "kijima": 18.5, "nagura": 14.7, "nishizaki": 14.7, "hikita": 17.9, "hachiman": 14.7, "hanawa": 20.0, "harashima": 17.5, "fukuzawa": 19.4, "isoda": 17.3, "inukai": 14.6, "hakamada": 14.6, "hiraishi": 14.6, "iwasawa": 21.9, "sasamoto": 14.6, "narumi": 14.6, "kamo": 14.5, "kometani": 14.5, "katsuyama": 14.5, "nishigaki": 14.5, "tsushima": 23.6, "hatori": 14.5, "nagaya": 14.5, "kanemoto": 14.4, "morino": 14.4, "shimamoto": 19.4, "murao": 14.3, "funabashi": 26.700000000000003, "uechi": 14.3, "sawamura": 21.3, "urakawa": 14.3, "shiro": 14.3, "kitani": 14.2, "muramoto": 17.5, "yamaura": 14.2, "iwahashi": 14.2, "cho": 18.9, "osugi": 14.1, "kami": 14.1, "akahori": 14.1, "odajima": 17.6, "koguma": 14.1, "katsura": 14.1, "osanai": 25.7, "wakasugi": 14.1, "shiba": 27.9, "hayase": 14.1, "kawamata": 22.6, "niisato": 14.0, "ise": 14.0, "ishige": 14.0, "soeda": 16.4, "yokomizo": 14.0, "tokita": 20.3, "takatsuka": 14.0, "takaku": 14.0, "itaya": 14.0, "serizawa": 19.3, "takamoto": 14.0, "miyamura": 14.0, "shiroma": 13.9, "fukagawa": 13.9, "nagamine": 27.9, "takeyama": 25.8, "hoshina": 16.2, "yanai": 31.6, "matsuba": 13.8, "iinuma": 13.7, "maruo": 13.7, "osako": 13.7, "naoi": 13.7, "chokai": 13.7, "hiraiwa": 13.7, "yonemura": 13.6, "taniyama": 13.6, "tokuyama": 13.6, "yamatani": 13.6, "nagamatsu": 13.6, "ido": 13.6, "yasuoka": 13.6, "kiriyama": 13.6, "takeshima": 16.0, "kunii": 13.5, "shiokawa": 13.5, "oue": 13.5, "mizukoshi": 13.5, "osuga": 13.4, "tsukuda": 13.4, "kambayashi": 22.6, "yamaji": 13.4, "beppu": 13.4, "tsuge": 13.4, "kurose": 13.4, "obama": 13.4, "akama": 13.4, "yasue": 13.4, "hyuuga": 13.4, "shinjou": 18.1, "igari": 13.3, "koda": 37.400000000000006, "iwano": 13.3, "ikoma": 13.3, "kumano": 13.3, "takenouchi": 13.3, "yaginuma": 13.3, "fukutome": 13.3, "oizumi": 13.2, "okochi": 13.2, "iwami": 18.2, "waki": 21.1, "aoshima": 13.1, "susaki": 13.1, "hirashima": 17.3, "murakawa": 13.1, "shinzan": 13.0, "fujie": 13.0, "fuji": 15.3, "chuhei": 13.0, "hayata": 13.0, "torigoe": 13.0, "fukami": 12.9, "uenishi": 12.9, "ishigami": 23.5, "natori": 12.9, "yamanishi": 12.9, "kumamoto": 17.1, "kamio": 12.9, "saruwatari": 12.9, "kamishima": 17.6, "hyoudou": 22.5, "akamine": 12.8, "kamakura": 12.8, "sakiyama": 12.8, "kozuka": 12.8, "komatsuzaki": 12.7, "watanuki": 12.7, "munakata": 21.299999999999997, "yahagi": 18.8, "sunada": 12.6, "haruda": 12.6, "washimi": 12.6, "iwakiri": 12.6, "choko": 18.2, "akiyoshi": 12.6, "matsusaka": 12.5, "taketani": 12.5, "shiobara": 12.5, "hagino": 12.5, "yasumoto": 12.5, "ishizu": 12.5, "sakuraba": 12.5, "ichihashi": 12.5, "onizuka": 12.4, "kajitani": 12.4, "niikura": 12.4, "sonobe": 15.9, "sekigawa": 12.4, "aratani": 12.4, "hiraki": 12.4, "mashita": 12.4, "hiruta": 12.4, "takekawa": 19.4, "okabayashi": 12.3, "hirooka": 14.700000000000001, "fujieda": 12.3, "ikeno": 12.3, "mase": 12.3, "nikaidou": 12.3, "toki": 12.2, "ooka": 12.2, "kagaya": 12.2, "sawara": 12.2, "takezawa": 19.8, "iso": 12.1, "isomura": 12.1, "higashida": 12.1, "tatsumi": 30.7, "kawajiri": 12.1, "kurosu": 12.0, "seike": 12.0, "toba": 12.0, "ichinose": 18.8, "kishino": 12.0, "tomoda": 12.0, "mabuchi": 15.0, "fuku-shima": 11.9, "takaishi": 11.9, "wakisaka": 11.9, "ohori": 11.9, "inutsuka": 11.9, "oga": 11.9, "omi": 22.2, "motoki": 19.9, "murano": 11.9, "horio": 11.9, "isaka": 16.400000000000002, "funatsu": 11.8, "kikugawa": 11.8, "yonekawa": 11.8, "hoshikawa": 11.8, "tomura": 11.8, "nakazono": 11.8, "okutani": 11.8, "sayama": 11.8, "yoshimatsu": 11.7, "suyama": 20.4, "tanioka": 11.7, "tamaru": 11.7, "wakatsuki": 14.899999999999999, "osato": 11.6, "hioki": 11.6, "yoshinari": 11.6, "taga": 11.6, "katakura": 11.6, "tsubochi": 11.6, "kitaguchi": 11.6, "houjou": 20.799999999999997, "kameya": 11.6, "ariyoshi": 11.6, "sanchi": 11.6, "kajikawa": 11.6, "odagiri": 18.8, "sugitani": 11.6, "tsujino": 11.6, "emoto": 11.5, "omoto": 18.4, "kawakita": 11.5, "miyatake": 11.5, "dokata": 11.5, "akazawa": 15.7, "ebisawa": 15.6, "daimon": 11.5, "hirosawa": 13.7, "shibahara": 16.4, "horibe": 11.4, "haruyama": 11.4, "owaki": 11.4, "inami": 21.8, "arimoto": 14.3, "takeishi": 13.700000000000001, "minowa": 14.3, "sada": 11.3, "fujikura": 11.3, "toya": 11.3, "ogiso": 11.3, "shitara": 11.3, "tayama": 11.3, "hosomi": 11.3, "okayasu": 11.3, "katahira": 11.2, "sokabe": 11.2, "himeno": 11.2, "tasaka": 11.2, "miyao": 11.2, "yashima": 13.5, "suehiro": 14.2, "akaishi": 11.2, "akao": 11.2, "asato": 11.2, "terayama": 11.2, "fukatsu": 11.1, "tsujita": 11.1, "nakagome": 11.1, "mamiya": 11.1, "kitaura": 11.1, "nakae": 11.1, "itoi": 11.1, "setoguchi": 11.1, "iseki": 13.2, "yokochi": 11.0, "sugie": 11.0, "egami": 11.0, "muta": 11.0, "takagaki": 11.0, "makabe": 11.0, "sakuta": 11.0, "wakai": 11.0, "toriyama": 11.0, "komatsubara": 11.0, "takamiya": 11.0, "hanzawa": 14.9, "tsuyuki": 10.9, "kanagawa": 10.9, "fujima": 10.9, "kira": 10.9, "suematsu": 10.9, "suetsugu": 10.9, "kawazu": 10.9, "furuno": 10.9, "kuniyoshi": 10.9, "shinkyo": 10.8, "kusama": 10.8, "hibi": 10.8, "okuhara": 10.7, "mine": 22.9, "kakuno": 10.7, "kamijou": 18.2, "asahi": 16.2, "futami": 10.7, "hashida": 10.7, "umeno": 10.6, "arikawa": 10.6, "shirota": 24.7, "miyama": 14.7, "takayoshi": 10.6, "iwakami": 10.6, "shimodaira": 10.6, "ejiri": 10.6, "washio": 10.6, "fujihira": 10.6, "sakane": 10.6, "nosaka": 10.6, "hayami": 12.7, "hayano": 10.5, "eikichi": 10.5, "tai": 10.5, "inagawa": 10.5, "yamamuro": 10.5, "hisamatsu": 10.5, "katori": 10.5, "maeyama": 10.4, "orihara": 10.4, "nishide": 10.4, "satomi": 10.4, "sase": 10.4, "okino": 10.4, "odawara": 10.4, "shirato": 17.0, "yaegashi": 10.4, "uesaka": 10.4, "ikenaga": 10.4, "nishikiori": 10.4, "sawaguchi": 14.4, "shinada": 10.4, "kiryuu": 10.4, "nakasaki": 10.4, "nabeshima": 10.3, "shouda": 15.5, "iwakura": 10.3, "ishimori": 10.3, "ino": 24.200000000000003, "hishinuma": 10.3, "kumasaki": 10.3, "imaoka": 10.3, "sasada": 10.3, "ikehara": 10.3, "yuda": 12.5, "miyabe": 10.2, "kogetsu": 10.2, "miyamae": 10.2, "izumitani": 10.2, "itsumi": 10.2, "shiroiwa": 10.2, "yuzawa": 13.7, "ejima": 10.1, "sakamaki": 17.5, "kimijima": 10.1, "koshiba": 10.1, "fuchigami": 16.4, "fujishiro": 18.1, "nishina": 10.1, "daidou": 10.1, "tateno": 22.9, "miyanaga": 10.1, "shimano": 13.399999999999999, "honjou": 19.1, "iio": 10.1, "onozuka": 10.0, "yamagiwa": 10.0, "nakase": 10.0, "sasaoka": 10.0, "takakuwa": 10.0, "kiyama": 10.0, "momma": 10.0, "narisawa": 13.6, "tano": 10.0, "nonomura": 9.9, "miyaoka": 9.9, "kizaki": 9.9, "kawakubo": 12.3, "nose": 19.200000000000003, "nakagaki": 9.9, "yanagimoto": 9.9, "matsufuji": 9.9, "kitabayashi": 9.9, "ishijima": 12.5, "sankaku": 9.9, "sumida": 9.9, "oguro": 9.9, "umezaki": 9.8, "asaoka": 12.8, "fukumura": 9.8, "akaike": 9.8, "sugata": 9.8, "ichise": 9.8, "aota": 9.8, "asari": 9.8, "hanyuu": 9.8, "sekido": 9.8, "matakichi": 9.7, "morooka": 13.2, "kashiwazaki": 9.7, "semba": 9.7, "funayama": 15.299999999999999, "ichinohe": 9.7, "furuse": 9.7, "tsukui": 9.7, "sasai": 9.7, "kitao": 9.7, "itami": 9.6, "matsumaru": 9.6, "shihou": 9.6, "uragami": 9.6, "isa": 9.6, "yatsushiro": 9.6, "mizokami": 9.6, "kusuda": 9.6, "tanji": 9.6, "nagatani": 9.6, "yusa": 9.6, "inose": 9.6, "wakasa": 9.6, "chugen": 9.6, "nakahashi": 9.5, "niizuma": 9.5, "hama-shima": 9.5, "mito": 11.6, "togawa": 16.7, "matsukura": 9.5, "tokoro": 9.5, "ohama": 9.5, "yamazoe": 9.5, "ogaki": 12.1, "kuwayama": 9.5, "fukase": 9.5, "gushiken": 9.5, "isshoku": 9.4, "katsuno": 9.4, "bessho": 9.4, "sugioka": 9.4, "tokiwa": 9.4, "haruna": 9.4, "iga": 9.4, "hiyama": 16.200000000000003, "ayabe": 9.4, "ogushi": 12.0, "uchikawa": 9.4, "noto": 9.4, "tateyama": 12.9, "anan": 9.3, "okui": 9.3, "takeno": 11.9, "kishikawa": 9.3, "nezu": 9.3, "hozumi": 9.3, "hikosaka": 9.3, "kasamatsu": 9.3, "hikichi": 9.3, "kuga": 9.3, "komaki": 9.2, "murota": 9.2, "okudaira": 9.2, "kogo": 9.2, "kamikawa": 9.2, "kitamoto": 9.2, "tanuma": 9.2, "tabe": 9.2, "minegishi": 17.2, "sugimori": 9.2, "asahina": 9.2, "enoki": 12.3, "kobayakawa": 9.1, "yunoki": 9.1, "akagawa": 9.1, "aramaki": 17.0, "oshida": 9.1, "uryuu": 9.1, "shiiba": 12.1, "ashi-sawa": 12.0, "sammei": 9.0, "sako": 25.1, "morohashi": 9.0, "ampo": 9.0, "iwao": 11.7, "ishigaya": 9.0, "urayama": 9.0, "komada": 9.0, "kunimoto": 9.0, "yoshizumi": 9.0, "ushita": 9.0, "kanemitsu": 9.0, "kurisu": 8.9, "ui": 8.9, "iiyama": 8.9, "miyai": 8.9, "kashiwa": 8.9, "yanaka": 8.9, "hiyoshi": 8.9, "sakuragi": 8.9, "shinno": 8.9, "yanagitani": 8.9, "terakado": 8.9, "kushita": 8.9, "kitai": 8.9, "sukegawa": 8.9, "maesawa": 12.3, "chujou": 17.200000000000003, "kiba": 8.9, "urushihara": 8.8, "tabei": 8.8, "kuwana": 8.8, "sumomo": 8.8, "tsugawa": 8.8, "mashima": 8.8, "genya": 8.8, "kajimoto": 8.8, "hishita": 8.8, "shinkai": 19.200000000000003, "ko": 16.8, "hiwatari": 8.8, "kadoma": 8.8, "sugi": 8.8, "tanizaki": 8.8, "uchibori": 8.7, "kiyokawa": 8.7, "nakatsuji": 8.7, "tanino": 8.7, "kumaki": 8.7, "tamiya": 8.7, "utsugi": 8.7, "sakagami": 8.7, "horinouchi": 8.7, "nakagami": 14.899999999999999, "imazu": 8.7, "kino": 8.7, "yokozawa": 11.2, "sakauchi": 8.6, "aoi": 8.6, "nakama": 10.899999999999999, "kameoka": 8.6, "anno": 8.6, "hashiba": 12.6, "yonemoto": 8.6, "shirahama": 8.6, "watabiki": 8.6, "futatsugi": 8.6, "hirade": 8.6, "yamanouchi": 8.6, "fujinaga": 8.6, "imaeda": 8.6, "fukawa": 15.399999999999999, "omachi": 8.6, "takayasu": 8.6, "yamabe": 13.6, "katsube": 8.5, "arata": 8.5, "osumi": 18.7, "uetake": 8.5, "gonda": 8.5, "sawano": 12.4, "awano": 8.5, "hasumi": 8.5, "horimoto": 8.5, "koyano": 8.5, "izumida": 8.5, "nakahata": 8.5, "nakatsu": 8.5, "fukano": 8.5, "yamakoshi": 10.7, "moriuchi": 8.5, "azumaya": 8.5, "jimpei": 8.5, "funada": 8.5, "wakui": 13.1, "hiroi": 8.4, "kohata": 8.4, "miyaguchi": 8.4, "tojou": 19.400000000000002, "togou": 14.8, "ichino": 8.4, "matsudaira": 8.4, "tagashira": 8.4, "suzumura": 8.4, "senhara": 8.4, "oide": 10.8, "okumoto": 8.4, "tsuno": 8.4, "mizuochi": 8.3, "takahama": 8.3, "yagishita": 11.0, "hiruma": 11.0, "suison": 8.3, "sakabe": 8.3, "mukaida": 8.3, "kawagishi": 8.3, "ushiyama": 8.2, "chino": 16.4, "miya": 8.2, "niki": 8.2, "takemori": 8.2, "daito": 8.2, "higo": 8.2, "uozumi": 8.2, "matsumiya": 8.2, "furuhon": 8.2, "saiki": 14.6, "koboku": 8.1, "ichida": 8.1, "iuchi": 8.1, "masumoto": 11.899999999999999, "mitamura": 8.1, "sassa": 8.1, "michishita": 8.1, "shinbori": 8.1, "hachiya": 12.5, "oto": 14.5, "sasayama": 8.1, "mae": 8.1, "minamino": 8.1, "usuda": 8.1, "orita": 8.1, "yagyuu": 8.1, "kakihara": 8.0, "akaboshi": 8.0, "shirouzu": 8.0, "maruya": 8.0, "tanizawa": 12.0, "nagahara": 13.7, "kurashima": 8.0, "ushio": 8.0, "hamamura": 8.0, "mino": 12.3, "nakaya": 18.2, "kirihara": 8.0, "noji": 10.5, "teramura": 8.0, "hiranuma": 7.9, "hanabusa": 7.9, "toyooka": 7.9, "fukamachi": 7.9, "kanezaki": 7.9, "sakakura": 7.9, "moroboshi": 7.9, "uzawa": 11.5, "iwakawa": 7.9, "kiguchi": 7.9, "daikoku": 7.9, "taka": 7.9, "yamamori": 7.9, "shirasaki": 7.9, "tange": 7.9, "okimoto": 7.9, "fujio": 7.9, "oike": 10.5, "shinke": 7.9, "takeichi": 10.7, "mitsumori": 7.8, "gondou": 7.8, "imazeki": 7.8, "yoshitomi": 10.8, "terakawa": 7.8, "mizusawa": 11.1, "onozawa": 10.4, "tokumaru": 7.8, "saga": 14.399999999999999, "miyaki": 7.8, "yoshiyama": 7.8, "fukuzaki": 7.8, "suganomiya": 7.8, "yonamine": 7.7, "ibaraki": 7.7, "yasutake": 7.7, "yonehara": 7.7, "nishiuchi": 7.7, "fukuma": 7.7, "emura": 7.7, "kasama": 7.7, "wakimoto": 7.7, "shiino": 7.7, "yasumura": 7.7, "yatsu": 7.7, "notsu": 7.7, "haruki": 7.7, "tooda": 7.7, "kawanaka": 7.7, "koiwa": 7.6, "inamoto": 7.6, "ichiyanagi": 7.6, "mizobuchi": 7.6, "nonoyama": 7.6, "kusaba": 7.6, "fukutomi": 9.8, "omote": 7.6, "minamikawa": 7.6, "koji": 9.899999999999999, "nagakura": 11.399999999999999, "araya": 7.6, "saijou": 25.799999999999997, "tozawa": 11.0, "ishimatsu": 7.5, "shirasaka": 7.5, "tsuru": 11.5, "miyakoshi": 7.5, "uema": 7.5, "kinukawa": 9.8, "nakade": 7.5, "hamaoka": 7.5, "nagae": 7.5, "yatabe": 11.2, "taoka": 7.5, "kusanagi": 7.5, "kashiwagura": 7.5, "miyagoshi": 7.4, "hatada": 10.100000000000001, "kuboki": 7.4, "hieda": 7.4, "ikui": 7.4, "yuge": 7.4, "sofue": 7.4, "kumasaka": 7.4, "oguchi": 7.4, "okiyama": 7.4, "furui": 7.4, "hanamura": 7.4, "onose": 7.4, "kajino": 7.4, "imanaka": 7.3, "ubukata": 7.3, "fukao": 7.3, "kashimura": 10.0, "itai": 7.3, "orikasa": 7.3, "tauchi": 7.3, "sannohe": 7.3, "kuwajima": 7.3, "tanifuji": 7.3, "kawashimo": 7.3, "terashita": 7.3, "sumiya": 10.4, "karube": 7.3, "shinomiya": 11.399999999999999, "nagatsuka": 11.2, "fujibayashi": 7.3, "koshida": 7.3, "uda": 7.2, "kominami": 7.2, "yamao": 7.2, "asaka": 10.8, "toyonaga": 7.2, "takebe": 10.1, "yabuuchi": 7.2, "seta": 7.2, "kumai": 7.2, "nomiyama": 7.2, "kagami": 16.099999999999998, "higaki": 12.3, "shintaku": 7.2, "toyokawa": 7.2, "sakae": 7.2, "sotoma": 7.2, "ojima": 7.2, "kuze": 7.2, "utsu": 7.2, "yokoe": 7.1, "hayama": 13.8, "negoro": 7.1, "chukan": 7.1, "seya": 7.1, "mishina": 7.1, "morimura": 7.1, "urasaki": 7.1, "kaboku": 7.1, "kani": 7.1, "masaoka": 9.5, "uetani": 7.1, "sasano": 7.1, "ogita": 7.1, "matsue": 13.899999999999999, "saotome": 14.1, "furuhata": 7.1, "kimata": 10.2, "kimizuka": 7.1, "katada": 7.1, "okutsu": 7.0, "kakei": 7.0, "kishigami": 7.0, "narikawa": 7.0, "kurabayashi": 7.0, "tonegawa": 7.0, "hemmi": 7.0, "sakairi": 7.0, "niihara": 7.0, "sumitomo": 7.0, "fukutani": 7.0, "atsuta": 7.0, "mikuni": 7.0, "higurashi": 7.0, "takechi": 9.3, "shishikura": 7.0, "ozasa": 7.0, "nogawa": 7.0, "kasutani": 7.0, "kohari": 7.0, "todaka": 7.0, "inaoka": 7.0, "nakauchi": 7.0, "kanematsu": 6.9, "hamaya": 6.9, "yuri": 10.4, "katsumi": 6.9, "tsunekawa": 6.9, "kushida": 6.9, "morozumi": 6.9, "kaneoka": 6.9, "obana": 6.9, "tanemura": 6.9, "kitami": 6.9, "ikezawa": 10.2, "yokote": 6.9, "narasaki": 6.9, "koshino": 6.9, "higashikawa": 6.9, "ike": 6.9, "otsuno": 6.9, "isoya": 6.9, "kinugasa": 6.9, "fujinuma": 6.9, "minoura": 6.9, "obuchi": 17.1, "kitade": 6.9, "tokichi": 6.8, "ikebe": 9.9, "ebina": 19.0, "watase": 6.8, "hakozaki": 6.8, "kayama": 13.5, "fujinami": 11.7, "horikiri": 6.8, "shirasawa": 9.2, "emori": 9.2, "masubuchi": 12.6, "kiso": 6.8, "kiyoshi": 6.8, "kamoshida": 6.8, "koiso": 6.8, "shirokura": 6.8, "asao": 9.0, "soba": 6.8, "kogata": 6.8, "akizuki": 6.8, "tsuchimoto": 6.8, "terasaka": 6.8, "tsunashima": 6.8, "kuboyama": 6.7, "yambe": 6.7, "jou": 6.7, "muroya": 10.0, "bito": 6.7, "mawatari": 6.7, "horino": 6.7, "kitatani": 6.7, "yonaha": 6.7, "seno": 6.7, "omuro": 6.7, "asahara": 6.7, "bamba": 6.7, "miyahira": 6.7, "miyanishi": 6.7, "uyama": 6.7, "taneta": 6.7, "minamitani": 6.7, "kadokura": 6.7, "misaki": 6.7, "hosoe": 6.7, "takeo": 6.7, "chikada": 6.7, "kizu": 6.7, "inayoshi": 6.7, "shima-tani": 6.7, "iizumi": 6.6, "morisawa": 9.3, "okazawa": 9.5, "iwahara": 6.6, "marui": 6.6, "hamakawa": 6.6, "todoroki": 6.6, "ukita": 6.6, "seguchi": 6.6, "yui": 12.6, "nakahama": 6.6, "nakamaru": 6.6, "takeshi": 6.6, "toida": 6.6, "masunaga": 6.5, "tsuchiyama": 6.5, "niina": 6.5, "abiko": 6.5, "ieda": 6.5, "dei": 6.5, "kozai": 6.5, "shinguu": 6.5, "tamamura": 6.5, "inage": 6.5, "gun": 6.5, "tadano": 8.6, "nishizuka": 6.5, "shuu": 6.5, "tan": 6.5, "kakegawa": 6.5, "saji": 6.5, "furushou": 6.5, "hiura": 6.5, "kawamitsu": 6.4, "higashihara": 6.4, "narushima": 6.4, "hiroshima": 6.4, "kitasaki": 6.4, "fukunishi": 6.4, "okai": 6.4, "anai": 6.4, "so-ta": 6.4, "kunieda": 6.4, "shimanuki": 6.4, "sugisawa": 6.4, "morishige": 6.4, "yashiro": 10.8, "ichioka": 6.4, "harayama": 6.4, "iwamatsu": 6.4, "kuroyanagi": 12.7, "hasunuma": 6.4, "samukawa": 6.4, "fukumori": 6.3, "todou": 6.3, "tsujikawa": 6.3, "marumoto": 6.3, "nishihata": 6.3, "kiyamu": 6.3, "iwatsuki": 6.3, "kiyofuji": 6.3, "takagishi": 6.3, "hirase": 6.3, "irisawa": 9.3, "urabe": 18.5, "shimozato": 6.3, "sekiyama": 6.3, "tomono": 12.3, "izutsu": 6.3, "uemoto": 9.3, "sampoku": 6.3, "kurauchi": 6.2, "tonosaki": 6.2, "nawa": 6.2, "yoshidome": 6.2, "nishinaka": 6.2, "yahiro": 6.2, "okukawa": 6.2, "niijima": 6.2, "nogi": 6.2, "minamide": 6.2, "yakushiji": 6.2, "mizunuma": 6.2, "ikehata": 9.7, "nagami": 6.2, "ezawa": 9.8, "hiroki": 6.2, "sasajima": 6.2, "yabuta": 8.4, "sugawa": 6.2, "saida": 6.2, "ugajin": 6.2, "watarai": 6.2, "watari": 6.2, "yoshie": 6.2, "shitsu": 6.2, "akasaki": 6.1, "inohara": 6.1, "toyofuku": 6.1, "kigawa": 6.1, "jinnai": 6.1, "akatsu": 6.1, "tokushige": 6.1, "tsurumaki": 6.1, "funado": 6.1, "mizota": 6.1, "fukahori": 6.1, "nishisaka": 6.1, "yokoji": 6.1, "chatani": 6.1, "ina": 6.1, "oshikawa": 6.1, "kawakatsu": 6.1, "wakao": 6.1, "nagahashi": 8.399999999999999, "kina": 6.0, "shibano": 10.6, "hitotsugi": 6.0, "tao": 6.0, "torigai": 6.0, "momiyama": 6.0, "hisatomi": 6.0, "yagihashi": 11.7, "maruhashi": 6.0, "tatematsu": 6.0, "shibusawa": 6.0, "kojou": 6.0, "tanii": 6.0, "ikuno": 6.0, "yonetsu": 6.0, "furuuchi": 6.0, "higashide": 6.0, "senda": 5.9, "kajiura": 5.9, "fuwa": 5.9, "oshio": 9.0, "chiga": 5.9, "kanamura": 5.9, "motomiya": 5.9, "amada": 5.9, "yamanashi": 5.9, "hanashima": 5.9, "moro": 5.9, "taura": 5.9, "nagoya": 10.3, "sabashi": 5.9, "gamou": 5.9, "makihara": 8.5, "tobe": 5.9, "maemura": 5.9, "shirono": 5.9, "mitsunaga": 5.9, "takeoka": 8.4, "tawara": 5.9, "taniwaki": 5.9, "kurosaka": 5.9, "hironaka": 10.4, "yamanoi": 5.9, "shigihara": 5.9, "shirakami": 5.9, "toma": 9.4, "korogi": 5.9, "amino": 5.9, "iwane": 5.9, "tsukiji": 5.9, "yokotani": 5.9, "tozaki": 5.9, "koto": 8.600000000000001, "saruta": 5.8, "momota": 5.8, "harima": 5.8, "taya": 5.8, "namerikawa": 5.8, "seshimo": 5.8, "sasamori": 5.8, "shoutaku": 5.8, "daijin": 5.8, "matsunami": 11.8, "daichu": 5.8, "momoi": 5.8, "tomii": 5.8, "motoyoshi": 9.3, "yamashina": 5.8, "seibu": 5.8, "shouno": 5.8, "kawato": 12.600000000000001, "shiroyama": 5.8, "ii": 9.3, "matsumori": 5.7, "iwanami": 5.7, "yogi": 5.7, "yoshitani": 5.7, "wakana": 5.7, "tokumoto": 8.3, "tago": 9.9, "tsukiyama": 5.7, "saika": 5.7, "takezaki": 5.7, "takarada": 5.7, "wakiyama": 5.7, "imafuku": 5.7, "housaka": 5.7, "nobe": 5.7, "kyouson": 5.7, "kaburaki": 7.9, "niinuma": 5.7, "awata": 5.7, "narimatsu": 5.7, "tahira": 5.7, "takane": 8.1, "morito": 10.0, "motoda": 5.7, "ujihara": 5.7, "ikejiri": 5.7, "nakatake": 5.7, "mitsuoka": 5.7, "kasugai": 5.6, "uekusa": 5.6, "tanishima": 5.6, "sagae": 5.6, "seko": 11.2, "namatame": 5.6, "numajiri": 5.6, "chubachi": 5.6, "tomitsuka": 5.6, "kanie": 5.6, "sasaya": 5.6, "takato": 5.6, "wako": 5.6, "nozue": 5.6, "seihou": 5.6, "sasakura": 5.6, "tachigi": 5.6, "akiya": 5.6, "ibuki": 5.6, "kidokoro": 5.6, "uchikoshi": 5.6, "kurachi": 9.8, "tsukioka": 5.6, "zama": 5.5, "domon": 5.5, "koitabashi": 5.5, "tokudome": 5.5, "enokida": 5.5, "aonuma": 5.5, "horiike": 5.5, "inao": 5.5, "umetani": 5.5, "komukai": 5.5, "katsurahara": 5.5, "sunohara": 5.5, "kawatsu": 5.5, "yagisawa": 5.5, "hagio": 5.5, "kakazu": 5.5, "kachi": 8.4, "hanashiro": 5.5, "otearai": 5.5, "echigo": 5.5, "ikemura": 5.5, "yao": 5.5, "takaesu": 5.5, "kakinoki": 5.5, "koishi": 5.5, "chonan": 5.4, "narahara": 5.4, "numakura": 5.4, "ushioda": 5.4, "shussui": 5.4, "yokokura": 5.4, "shirone": 5.4, "sannomiya": 5.4, "gima": 5.4, "akada": 5.4, "tomatsu": 5.4, "nyuuzan": 5.4, "taketomi": 5.4, "chibana": 5.4, "hashino": 5.4, "kizawa": 5.4, "inatomi": 5.4, "sawaki": 5.4, "tomikawa": 5.4, "ariyama": 5.4, "miyazono": 5.4, "murofushi": 5.4, "nakagoshi": 5.4, "kanauchi": 5.4, "kurasawa": 5.4, "koketsu": 5.4, "hirako": 5.4, "tomohisa": 5.4, "nanri": 5.4, "maru": 5.4, "ikai": 5.3, "komagata": 5.3, "saikai": 5.3, "itani": 5.3, "takama": 5.3, "sugasawa": 7.6, "hakusan": 5.3, "saigou": 5.3, "haku": 8.7, "kuronuma": 5.3, "onosato": 5.3, "hanamoto": 5.3, "sasao": 5.3, "masuoka": 5.3, "hamachi": 5.3, "asaga": 5.3, "ikezaki": 5.3, "itokawa": 5.3, "kambara": 5.3, "ikeguchi": 5.3, "yokose": 5.3, "tosa": 5.3, "agatsuma": 5.3, "araida": 5.3, "maruno": 5.3, "kozono": 5.3, "ishitobi": 5.3, "tohan": 5.3, "moriki": 5.3, "hosogai": 5.3, "hamabe": 5.3, "yokotsuka": 5.3, "akino": 5.3, "inoshita": 5.3, "sera": 5.3, "tsuruya": 5.2, "kuriki": 9.7, "kenji": 10.3, "sakita": 5.2, "hayasaki": 5.2, "tsujioka": 5.2, "kaizuka": 5.2, "morigami": 5.2, "tsuburaya": 5.2, "hadano": 5.2, "hanazawa": 8.2, "nishibayashi": 5.2, "takino": 5.2, "yuyama": 5.2, "hosogi": 5.2, "omine": 5.2, "sezaki": 5.2, "fukuya": 5.2, "taikin": 5.2, "takehana": 5.2, "sado": 5.2, "ijichi": 5.2, "sasanuma": 5.2, "nishidaira": 5.2, "inai": 5.2, "umebayashi": 5.2, "sugishita": 5.2, "kamoshita": 5.2, "koriyama": 5.2, "harikai": 5.2, "shimojou": 8.7, "hisashita": 5.2, "yamasawa": 7.7, "horiba": 5.2, "tanada": 5.2, "kamekawa": 5.2, "sueta": 5.1, "jinguu": 5.1, "akutagawa": 5.1, "kezuka": 5.1, "shino": 5.1, "isogawa": 5.1, "izuhara": 5.1, "mizuma": 5.1, "azuma": 5.1, "takeguchi": 5.1, "nabeta": 5.1, "umeyama": 5.1, "nawata": 5.1, "tanigaki": 5.1, "hisahara": 5.1, "kadoi": 5.1, "sugii": 5.1, "saita": 7.5, "anazawa": 5.1, "tachihara": 5.1, "hagiya": 5.1, "kozuki": 5.1, "hisanaga": 5.1, "mima": 5.1, "sunahara": 5.1, "kitta": 5.1, "zaitsu": 5.1, "arisaka": 5.1, "yagura": 5.1, "kurono": 5.1, "yokobori": 5.1, "hoshiba": 5.0, "kawate": 5.0, "miyamori": 5.0, "umekawa": 5.0, "shimaoka": 7.5, "iioka": 5.0, "isoyama": 5.0, "ichiki": 5.0, "uchima": 5.0, "ashi-kawa": 5.0, "iwayama": 5.0, "kure-ya": 5.0, "motoyama": 5.0, "natsui": 5.0, "konaka": 5.0, "hariya": 5.0, "nakauma": 5.0, "shibuta": 5.0, "mitsuzuka": 5.0, "momohara": 5.0, "yuhara": 5.0, "itano": 5.0, "wakahara": 5.0, "kawanabe": 5.0, "toshima": 9.1, "murasaki": 5.0, "kamagata": 5.0, "sengoku": 5.0, "masukawa": 5.0, "katsuki": 5.0, "mokubu": 5.0, "koge": 5.0, "kamishiro": 7.2, "tsuneta": 5.0, "ohase": 4.9, "kariya": 16.0, "yomogida": 4.9, "kunihiro": 4.9, "takabe": 4.9, "hamana": 4.9, "mizuhara": 4.9, "arao": 4.9, "kamitani": 4.9, "tobari": 4.9, "shigemura": 4.9, "sekizawa": 4.9, "shigeno": 8.2, "takatori": 8.3, "awaji": 4.9, "iwahori": 4.9, "sugio": 4.9, "onaga": 4.9, "ichinomiya": 4.9, "itokazu": 4.9, "amma": 4.9, "kaminaka": 4.9, "shimane": 4.9, "kitazume": 4.9, "tonomura": 7.300000000000001, "itoga": 4.9, "wakamiya": 4.9, "hoshiyama": 4.9, "ishidou": 4.9, "shitsuoka": 4.8, "tokuhara": 4.8, "mizuki": 4.8, "yamazato": 4.8, "hachisuka": 4.8, "tanahara": 4.8, "yamafuji": 4.8, "tamano": 4.8, "oyagi": 4.8, "idemura": 4.8, "doumoto": 4.8, "aidajuu": 4.8, "takamisawa": 4.8, "kujuu": 4.8, "kawanami": 7.199999999999999, "koshimizu": 7.6, "inamori": 4.8, "imasato": 4.8, "kakui": 4.8, "doujou": 4.8, "ebata": 4.8, "sachi": 4.8, "kikuya": 4.8, "kuroishi": 4.8, "tsusaki": 4.8, "tsuyama": 4.8, "yokoshima": 4.8, "kanehira": 7.5, "yamaga": 4.7, "ehata": 9.100000000000001, "osono": 4.7, "abiru": 4.7, "iriguchi": 4.7, "tatsuta": 7.300000000000001, "yokosuka": 4.7, "shibagaki": 4.7, "kamogawa": 4.7, "imajou": 4.7, "sugou": 6.9, "hanatani": 4.7, "kashimoto": 4.7, "uesato": 4.7, "yasukochi": 4.7, "gen": 4.7, "takaya": 4.7, "murashima": 4.7, "futakawa": 4.7, "kaminaga": 8.7, "kora": 4.7, "kunitake": 4.7, "kamitaira": 4.7, "tokura": 4.7, "minoda": 4.7, "kaba-shima": 4.7, "nakasuji": 4.7, "nukii": 4.7, "ishido": 4.7, "arase": 4.7, "umakoshi": 4.7, "kusuhara": 4.7, "fujiu": 4.7, "kawasumi": 7.300000000000001, "sawabe": 4.7, "shibukawa": 4.7, "katsuragawa": 4.7, "arisawa": 4.7, "yoshiura": 4.6, "shimooka": 4.6, "niitsu": 4.6, "nagaishi": 4.6, "akanuma": 4.6, "chutai": 4.6, "sakizaki": 4.6, "kubodera": 4.6, "miyatani": 4.6, "yokozeki": 4.6, "kabeya": 4.6, "nanjou": 8.399999999999999, "fujimaru": 4.6, "uesu": 4.6, "uchita": 4.6, "akase": 4.6, "kanagi": 4.6, "fujinaka": 4.6, "numao": 4.6, "honkawa": 4.6, "suizu": 4.6, "yakuwa": 4.6, "nishibori": 4.6, "shirahata": 4.6, "yamamichi": 4.6, "teraguchi": 4.6, "anraku": 4.6, "yasutomi": 4.6, "takahira": 8.1, "osamura": 4.6, "kanke": 4.6, "mitsuishi": 4.6, "shimojima": 4.5, "mashio": 4.5, "sekihara": 4.5, "ikeyama": 4.5, "mitsuda": 4.5, "shigemoto": 4.5, "kokai": 4.5, "aburaya": 4.5, "kaito": 4.5, "takii": 4.5, "ideta": 4.5, "fukushige": 4.5, "sawazaki": 4.5, "an-shima": 4.5, "tarui": 4.5, "ue": 4.5, "ikedo": 4.5, "kawarazaki": 4.5, "sogawa": 4.5, "kiritani": 4.5, "oketani": 4.5, "furutachi": 8.6, "hane": 4.5, "matsudo": 4.5, "motoya": 4.5, "furusato": 4.5, "karaki": 4.5, "kunimatsu": 4.5, "nashimoto": 4.5, "jitsukawa": 4.5, "antaku": 4.5, "katoono": 4.4, "katayose": 4.4, "yokono": 4.4, "taneichi": 4.4, "shinzaki": 4.4, "harasawa": 4.4, "obi": 4.4, "kusakari": 4.4, "matsukuma": 4.4, "ose": 4.4, "muguruma": 4.4, "yura": 4.4, "maehata": 4.4, "tsukazaki": 4.4, "daichi": 4.4, "mashino": 4.4, "muraishi": 4.4, "miyabayashi": 4.4, "terunuma": 4.4, "usuki": 4.4, "mizui": 4.4, "hisamoto": 4.4, "iriya": 4.4, "sankichi": 4.4, "masuzawa": 4.4, "kashiwaya": 4.4, "kyuujou": 4.4, "amari": 4.4, "kuraoka": 4.4, "katsumura": 4.4, "shitaya": 4.4, "orui": 4.4, "tomaru": 4.4, "shimobara": 4.4, "nishinomiya": 4.4, "yoshiike": 4.4, "watatani": 4.4, "iwaoka": 4.4, "awazu": 4.4, "marumo": 4.4, "ryuu": 4.4, "kashihara": 4.4, "mitomi": 4.4, "ichijou": 7.7, "hon-shima": 4.4, "ogi": 13.8, "yamai": 4.4, "yuudou": 4.4, "inoguchi": 4.3, "kakita": 4.3, "onozaki": 4.3, "naoe": 4.3, "takubo": 8.6, "kasa": 4.3, "morota": 4.3, "shinnou": 4.3, "yoshimori": 4.3, "mizukawa": 4.3, "morihara": 4.3, "nakaguchi": 4.3, "urashima": 4.3, "yamahara": 4.3, "kanari": 4.3, "kanasaka": 4.3, "amou": 4.3, "sueoka": 4.3, "hirohata": 4.3, "innami": 4.3, "kawaji": 4.3, "mon": 4.3, "higashiguchi": 4.3, "sasa": 4.3, "shuuto": 4.3, "miyara": 4.3, "uegaki": 4.3, "omatsu": 4.3, "kumon": 4.3, "ayukawa": 4.3, "seijou": 4.3, "makiyama": 4.3, "kujirai": 4.3, "minemura": 7.1, "uotani": 4.3, "azegami": 4.3, "toyohara": 4.3, "emi": 4.3, "ikushima": 4.3, "sandai": 4.3, "kumakawa": 4.2, "otsuji": 6.4, "kadono": 4.2, "nakatsukasa": 8.4, "kamide": 4.2, "kakizawa": 4.2, "koshio": 4.2, "rikiishi": 4.2, "koeda": 4.2, "ichige": 4.2, "yabu": 4.2, "iidaka": 4.2, "shimba": 4.2, "ikejima": 4.2, "kyoutani": 4.2, "funamoto": 4.2, "kittaka": 4.2, "komoda": 8.4, "okado": 4.2, "taiga": 4.2, "mio": 4.2, "kane-ke-ko": 4.2, "mayuzumi": 4.2, "okawara": 4.2, "morizono": 4.2, "kon": 4.2, "kurebayashi": 4.2, "hirukawa": 4.2, "yoshio": 4.2, "higashimoto": 4.2, "motooka": 4.2, "katata": 4.2, "imabayashi": 4.2, "uramoto": 4.2, "tsumagari": 4.2, "tone": 4.2, "tokuoka": 4.2, "yoshimizu": 4.2, "naganawa": 4.2, "ebine": 4.2, "izaki": 7.2, "tama-naha": 4.2, "hanasaki": 4.2, "shiiki": 4.2, "ariizumi": 4.2, "ishizeki": 4.2, "irei": 4.2, "arii": 4.2, "koyama": 7.2, "orito": 4.2, "sawamoto": 6.5, "kozaki": 4.1, "katayanagi": 4.1, "enoto": 4.1, "shijou": 4.1, "namegata": 4.1, "shima-kawa": 4.1, "ukawa": 4.1, "takemasa": 4.1, "yoshihashi": 4.1, "kuratani": 4.1, "shioiri": 4.1, "fukamizu": 4.1, "nishikubo": 4.1, "fujitsuka": 4.1, "kuwamura": 4.1, "aho": 4.1, "misono": 4.1, "susumu": 4.1, "hakoda": 4.1, "shikanai": 4.1, "shoukoku": 4.1, "matsuhisa": 4.1, "wakagi": 4.1, "ike-ke-tani": 4.1, "moriyasu": 6.5, "kuge": 4.1, "ko-hen": 4.1, "ezure": 4.1, "hirotani": 4.1, "gei": 4.1, "gouhara": 4.1, "niwano": 4.1, "niioka": 4.1, "kozu": 4.1, "teratani": 4.1, "an": 4.1, "inaki": 4.1, "oze": 4.1, "kitabatake": 7.0, "tammachi": 4.1, "okaniwa": 4.1, "kunida": 4.1, "kambe": 4.0, "suita": 4.0, "otabe": 4.0, "agui": 4.0, "tsuchikawa": 4.0, "shigeoka": 4.0, "majima": 4.0, "kobashigawa": 4.0, "obinata": 7.7, "tochigi": 6.3, "komaba": 4.0, "urushibata": 4.0, "yabushita": 4.0, "higashijima": 4.0, "itsukida": 4.0, "anada": 4.0, "tawada": 4.0, "sakazaki": 4.0, "aragane": 4.0, "kawachi": 4.0, "ikeshita": 4.0, "nishimaki": 6.8, "ishibe": 4.0, "shikkou": 4.0, "takasuga": 4.0, "takigami": 4.0, "furuike": 4.0, "tateiwa": 4.0, "kuji": 4.0, "oshikiri": 4.0, "takatsuki": 4.0, "dan-ue": 4.0, "haryuu": 4.0, "shinoki": 4.0, "ikegawa": 4.0, "nakayasu": 4.0, "toji": 4.0, "fukukawa": 4.0, "terachi": 4.0, "chiyo": 4.0, "taniai": 4.0, "jouka": 4.0, "sanbon": 3.9, "tsukano": 3.9, "honzawa": 6.199999999999999, "fujise": 3.9, "toguchi": 10.4, "kunitomo": 3.9, "katsuragi": 3.9, "kuro-shima": 3.9, "taba": 3.9, "nozoe": 3.9, "kochi": 7.8, "izumo": 3.9, "murasawa": 3.9, "jouko": 3.9, "hikasa": 3.9, "iiboshi": 3.9, "kanao": 3.9, "izumikawa": 3.9, "takishita": 3.9, "mizobe": 3.9, "fukae": 3.9, "masuya": 3.9, "nabetani": 3.9, "ijiri": 3.9, "yasufuku": 3.9, "kanki": 3.9, "katsurada": 3.9, "kiyuna": 3.9, "hishikawa": 3.9, "oyabu": 7.4, "setoyama": 3.9, "ikari": 3.9, "tatebe": 3.9, "misu": 3.9, "oshino": 3.9, "nakachi": 3.9, "mikawa": 7.699999999999999, "suwabe": 3.9, "shima-nai": 3.9, "teramae": 3.9, "hasuike": 3.9, "mandai": 3.9, "sen-shima": 3.9, "daigo": 3.9, "sakemi": 3.9, "kawabuchi": 3.9, "katsumoto": 3.9, "yasuhiko": 3.9, "minamisawa": 3.9, "aiba": 3.9, "inokuma": 3.9, "imahashi": 3.9, "haji": 3.9, "usuta": 3.9, "higashimura": 3.9, "horigome": 9.600000000000001, "yotsumoto": 3.9, "degawa": 3.9, "hamamatsu": 3.9, "doumae": 3.9, "agarie": 3.9, "mizobata": 3.8, "hanano": 3.8, "kamura": 3.8, "edo": 3.8, "naka-shima": 3.8, "ebato": 3.8, "murabayashi": 3.8, "asayama": 6.6, "shiratani": 3.8, "sashida": 3.8, "furukoori": 3.8, "kashiwada": 3.8, "nagamori": 6.4, "wachi": 9.1, "morio": 3.8, "kikumoto": 3.8, "mineta": 3.8, "hirado": 3.8, "koseki": 3.8, "kitera": 3.8, "kunugi": 3.8, "haba": 6.3, "togami": 3.8, "kaibara": 3.8, "waragai": 3.8, "koyasu": 3.8, "ichimaru": 3.8, "nagato": 3.8, "moroi": 3.8, "kainuma": 3.8, "minamiguchi": 3.8, "amaya": 3.8, "chuzen": 3.8, "amakawa": 3.8, "uezono": 3.8, "higashiura": 3.8, "kikkawa": 3.8, "kontani": 3.8, "imanari": 3.8, "sakanishi": 3.8, "hyakutake": 3.8, "kanasugi": 3.8, "makimura": 3.8, "hiratani": 3.8, "shimakura": 3.8, "douguchi": 3.8, "terawaki": 3.7, "sakurazawa": 3.7, "hoashi": 3.7, "inayama": 3.7, "okahara": 3.7, "kabasawa": 3.7, "sakihama": 3.7, "kuni-shima": 3.7, "shuu-shima": 3.7, "koshiishi": 3.7, "tanigami": 3.7, "onoki": 3.7, "kadokawa": 3.7, "yokohama": 3.7, "chiyoda": 3.7, "aoto": 6.300000000000001, "okitsu": 6.6, "iura": 3.7, "tsunakawa": 3.7, "uenoyama": 3.7, "kitagaki": 3.7, "kamoda": 3.7, "mochimaru": 3.7, "muneda": 3.7, "nukaga": 3.7, "kusunose": 3.7, "kamisawa": 6.2, "toyosaki": 3.7, "hokazono": 3.7, "hasui": 3.7, "watai": 3.7, "nomiya": 3.7, "itoyama": 3.7, "shigenobu": 3.7, "myoujin": 3.7, "miyashiro": 3.7, "miyairi": 3.7, "kajimura": 3.7, "okunishi": 3.7, "fukuzumi": 3.7, "kawaharada": 3.7, "nanzan": 3.7, "nakato": 3.7, "takeshi-kasa": 3.7, "tsuchie": 3.7, "shimosaka": 3.7, "negi": 3.7, "nakatsugawa": 3.7, "shiozuka": 3.7, "kyougoku": 3.7, "hatae": 3.7, "sakatsume": 3.7, "hayashibara": 3.7, "numazaki": 3.7, "kominato": 3.6, "sakihara": 3.6, "satomura": 3.6, "nakaji": 6.2, "futagami": 6.5, "kaise": 3.6, "naya": 3.6, "gyoubu": 3.6, "miyaura": 3.6, "kaizu": 3.6, "mitsumoto": 3.6, "numano": 3.6, "hiroe": 3.6, "hikawa": 3.6, "tomonaga": 6.300000000000001, "mieno": 3.6, "tomomatsu": 3.6, "ideno": 3.6, "aiko": 3.6, "tokai": 3.6, "boku": 3.6, "okusa": 3.6, "takegami": 3.6, "magara": 3.6, "soraku": 3.6, "mayumi": 3.6, "watari-keiji": 3.6, "kazami": 3.6, "nakagawara": 3.6, "komoriya": 3.6, "imamoto": 3.6, "kitawaki": 3.6, "kasa-shima": 3.6, "yonemitsu": 3.6, "okusawa": 3.6, "yara": 3.6, "higami": 3.6, "sumimoto": 3.6, "nishizono": 3.6, "ema": 3.6, "kure": 3.6, "tamoto": 3.6, "karyuu": 3.6, "oshika": 3.6, "higano": 3.6, "nakagusuku": 3.6, "ikematsu": 3.6, "tokui": 3.6, "moribe": 3.6, "habu": 3.6, "minematsu": 3.6, "taki-shima": 3.6, "eifuu": 3.6, "takekoshi": 6.1, "taguma": 3.6, "sakaino": 3.6, "kindaka": 3.6, "fueki": 3.6, "haneishi": 3.6, "osone": 5.9, "nishie": 3.6, "kushima": 3.6, "houya": 3.6, "tamayama": 3.6, "odate": 6.1, "masago": 3.6, "hashikawa": 3.6, "wakuta": 3.6, "okimura": 3.6, "fujishita": 3.6, "yamase": 3.5, "katsuro": 3.5, "taisei": 3.5, "nagahata": 3.5, "oshige": 3.5, "toku-shima": 3.5, "hihara": 3.5, "era": 3.5, "douzono": 3.5, "murahashi": 3.5, "shigemori": 3.5, "shirasu": 3.5, "tonai": 3.5, "morinaka": 3.5, "iizawa": 3.5, "kamata": 6.8, "kuki": 3.5, "mitsugi": 3.5, "shikata": 3.5, "kaida": 3.5, "nama-numa": 3.5, "kurisaki": 3.5, "yamahira": 3.5, "ioka": 3.5, "shiba-hon": 3.5, "takusan": 3.5, "tomiyasu": 3.5, "kamikubo": 3.5, "hashi": 3.5, "kisawa": 3.5, "chin": 3.5, "shimpon": 3.5, "koki": 3.5, "kojika": 3.5, "miyayama": 3.5, "ebi-numa": 3.5, "sonoyama": 3.5, "misaka": 3.5, "yuguchi": 3.5, "kamii": 3.5, "hamai": 3.5, "inafuku": 3.5, "minamida": 3.5, "kurobe": 3.5, "nobata": 3.5, "mifune": 3.5, "nakakura": 3.5, "kosakai": 3.5, "oyaizu": 3.5, "suhara": 3.5, "nakakita": 3.5, "ikegame": 3.5, "nakamatsu": 6.0, "furue": 3.5, "kawafuku": 3.5, "yamanobe": 3.5, "tsurusaki": 3.5, "ideguchi": 3.4, "ashikaga": 3.4, "kudaka": 3.4, "ishiwata": 3.4, "tooya": 3.4, "tokunou": 3.4, "iba": 6.5, "okamatsu": 3.4, "suetake": 3.4, "tomobe": 3.4, "shiki": 3.4, "ninagawa": 3.4, "ogi-shima": 3.4, "teshigawara": 3.4, "shu": 3.4, "hirobe": 3.4, "nouchi": 3.4, "nakamizo": 3.4, "tarumi": 6.0, "maebashi": 3.4, "uko": 3.4, "moriizumi": 3.4, "naruke": 3.4, "furukata": 3.4, "uji": 3.4, "kurotaki": 3.4, "ushiroda": 3.4, "kitanaka": 3.4, "musha": 3.4, "tokumitsu": 3.4, "kuramitsu": 3.4, "sumikawa": 3.4, "tanoue": 3.4, "fujihashi": 3.4, "hangai": 3.4, "kasagi": 3.4, "karimata": 3.4, "tomimatsu": 3.4, "tazoe": 3.4, "ni-no-miya": 3.4, "iyama": 3.4, "kawadaira": 3.4, "tokashiki": 3.4, "some-ya": 3.4, "ibe": 3.4, "mitome": 3.4, "ashino": 3.4, "housen": 3.4, "aiuchi": 3.4, "yamahata": 3.4, "kukita": 3.4, "sakanashi": 3.4, "takahagi": 3.4, "someno": 3.4, "hagita": 3.4, "toge": 3.4, "kosuda": 3.4, "chikazawa": 3.4, "hamasuna": 3.4, "oniki": 3.4, "ishihama": 3.4, "katase": 3.4, "yoshinaka": 3.4, "tokutake": 3.4, "akaiwa": 3.4, "irino": 3.3, "okonogi": 3.3, "hai-shima": 3.3, "toyabe": 3.3, "ne-kan": 3.3, "uchiki": 3.3, "seyama": 3.3, "uemori": 3.3, "wara-ka": 3.3, "kanakubo": 3.3, "sugiki": 3.3, "ichi-shi": 3.3, "kawatani": 3.3, "kazuhisa": 3.3, "masumura": 3.3, "kanazashi": 3.3, "nakashita": 3.3, "amanuma": 3.3, "mitsuse": 3.3, "ikema": 3.3, "shimozawa": 3.3, "harakawa": 3.3, "to-makoto": 3.3, "masutani": 3.3, "kagen": 3.3, "urai": 3.3, "mesaki": 3.3, "kushibiki": 3.3, "kuroi": 3.3, "morimitsu": 3.3, "kasaya": 3.3, "tamari": 3.3, "tsubokura": 3.3, "ichiyama": 3.3, "machino": 3.3, "mino-shima": 3.3, "imayoshi": 3.3, "murashita": 3.3, "sodeyama": 3.3, "yokomori": 3.3, "muraguchi": 3.3, "suzue": 3.3, "nobuhara": 3.3, "kuwahata": 3.3, "shigetomi": 3.3, "iimori": 5.699999999999999, "izumiyama": 3.3, "o": 3.3, "bajou": 3.3, "kamiguchi": 3.3, "kugimiya": 3.3, "anjuu": 3.3, "otobe": 3.3, "komazawa": 3.3, "hama-moto": 3.3, "miyakuni": 3.3, "shita-nishi": 3.3, "washino": 3.3, "kiyose": 3.3, "kuwamoto": 3.3, "kurashige": 3.2, "sawatani": 3.2, "enami": 3.2, "yabata": 3.2, "ganaha": 3.2, "tsubone": 3.2, "mera": 3.2, "komata": 3.2, "sakuramoto": 3.2, "hyaku-(kurikaesi)": 3.2, "narukiyo": 3.2, "habara": 3.2, "arioka": 3.2, "nakagiri": 3.2, "ashihara": 3.2, "sakao": 3.2, "sunami": 3.2, "tanzawa": 3.2, "matano": 3.2, "hayashizaki": 3.2, "noutomi": 3.2, "sakura": 3.2, "ogiya": 3.2, "yashiki": 3.2, "nita": 3.2, "nishihama": 3.2, "amitani": 3.2, "suzaki": 3.2, "mitsuyasu": 3.2, "marukawa": 3.2, "mitoma": 3.2, "kuniyasu": 3.2, "kobu": 3.2, "fumoto": 3.2, "mineo": 5.9, "oneda": 3.2, "okaya": 3.2, "kamezaki": 3.2, "oyakawa": 3.2, "tomoyori": 3.2, "hikima": 3.2, "kuzuya": 3.2, "yachi": 3.2, "zushi": 3.2, "serikawa": 3.2, "gyouda": 3.2, "takaichi": 3.2, "kurino": 3.2, "senuma": 3.2, "sekizuka": 3.2, "nakanowatari": 3.2, "tsunami-ko": 3.2, "kurusu": 3.2, "yasumatsu": 3.2, "iwasaka": 3.2, "yokomichi": 3.2, "aki": 3.2, "temma": 3.2, "chinone": 3.2, "kaiho": 3.2, "shimode": 3.2, "sunayama": 3.2, "fujimatsu": 3.2, "akune": 3.2, "nasuno": 3.2, "hazama": 5.4, "kaita": 3.2, "fujito": 3.2, "ijima": 3.2, "hisayama": 3.2, "shinchi": 3.2, "koreeda": 3.1, "iwa-shima": 3.1, "tagaya": 3.1, "kawa-bu": 3.1, "korenaga": 3.1, "numaguchi": 3.1, "saku-hon": 3.1, "matsuishi": 3.1, "kajiya": 3.1, "nito": 3.1, "hoshida": 3.1, "yanaga": 3.1, "kunisawa": 3.1, "niizeki": 3.1, "nunome": 3.1, "ariki": 3.1, "tanihara": 3.1, "fujiura": 3.1, "miyadera": 3.1, "shiotsuki": 3.1, "yoshiki": 3.1, "hiru-kan": 3.1, "fukumitsu": 3.1, "waga": 3.1, "maruko": 3.1, "hagimoto": 3.1, "kajii": 3.1, "amamoto": 3.1, "fushiki": 3.1, "serita": 3.1, "kamachi": 3.1, "odagawa": 3.1, "yasaka": 3.1, "matsunobu": 3.1, "ajiro": 3.1, "fujikake": 3.1, "egusa": 3.1, "kadoguchi": 3.1, "kiyosawa": 3.1, "nagahori": 3.1, "onohara": 3.1, "kamaya": 3.1, "kayano": 3.1, "ana-ken": 3.1, "takura": 3.1, "sasabe": 3.1, "saso": 5.5, "iyoda": 3.1, "akinaga": 3.1, "takagawa": 3.1, "koba": 3.1, "tonooka": 3.1, "uratani": 3.1, "tatsuno": 3.1, "irikura": 3.1, "inazawa": 3.1, "inamine": 3.1, "kuboi": 3.1, "yonei": 3.1, "narui": 3.1, "kemmoku": 3.1, "sakuyama": 3.1, "chiaki": 3.1, "annaka": 3.0, "matsumae": 3.0, "inou": 3.0, "shiozu": 3.0, "shinji": 3.0, "itatsu": 3.0, "hirate": 3.0, "shio-shima": 3.0, "nohira": 3.0, "hagi": 3.0, "genshi": 3.0, "ya-nochi": 3.0, "shiromoto": 3.0, "yamate": 3.0, "i": 3.0, "kahoku": 3.0, "naraoka": 3.0, "tateoka": 3.0, "kura": 3.0, "sakasai": 3.0, "shita": 3.0, "iseda": 3.0, "nanaumi": 3.0, "tsubokawa": 3.0, "takanezawa": 3.0, "kusakawa": 3.0, "shukuya": 3.0, "nagakubo": 3.0, "arashi": 3.0, "shibatani": 3.0, "karida": 3.0, "aoya": 3.0, "tsuyusaki": 3.0, "takashi-shima": 3.0, "komesu": 3.0, "kosoboku": 3.0, "shima-shiri": 3.0, "tatara": 3.0, "arimatsu": 3.0, "nagaki": 3.0, "yoshikoshi": 3.0, "katsuma": 3.0, "suemitsu": 3.0, "inakuma": 3.0, "kisanuki": 3.0, "suzukawa": 3.0, "minobe": 3.0, "tokuno": 3.0, "koza": 3.0, "kage": 3.0, "hinokuma": 3.0, "san-hori": 3.0, "tsuboya": 3.0, "kawaura": 3.0, "fuda": 3.0, "shibui": 3.0, "takenaga": 3.0, "takiya": 3.0, "esaka": 3.0, "nakamura-kyo": 3.0, "isayama": 5.5, "hosoyama": 3.0, "tsunematsu": 5.9, "kumabe": 3.0, "mizoe": 3.0, "sakaida": 3.0, "takesue": 3.0, "kabaya": 3.0, "kadomoto": 3.0, "to": 2.9, "kitakami": 2.9, "tokuhisa": 2.9, "iwawaki": 2.9, "shigeto": 2.9, "machii": 2.9, "terato": 2.9, "hamagami": 2.9, "tsumita": 2.9, "takasago": 2.9, "kohon": 2.9, "nono-kaki": 2.9, "koinuma": 5.699999999999999, "jibiki": 2.9, "tomonari": 2.9, "maetani": 2.9, "susa": 5.3, "enzan": 2.9, "hano": 2.9, "kawade": 2.9, "kikui": 2.9, "norimatsu": 2.9, "sakon": 2.9, "yagawa": 2.9, "ei": 2.9, "musashi": 2.9, "motoi": 5.4, "itasaka": 2.9, "hanayama": 2.9, "somekawa": 2.9, "tokue": 2.9, "makiuchi": 2.9, "nagane": 2.9, "okudera": 2.9, "matsuse": 2.9, "okuri": 2.9, "mitsuya": 2.9, "sanwa": 2.9, "chaki": 2.9, "kitagou": 2.9, "washida": 2.9, "kama": 2.9, "tanjou": 2.9, "hosonuma": 2.9, "kutsuzawa": 2.9, "ako": 2.9, "terabayashi": 2.9, "kusayanagi": 2.9, "arimitsu": 2.9, "ko-koku": 2.9, "une": 2.9, "eikawa": 2.9, "kuroba": 2.9, "joushima": 2.9, "shigenaga": 2.9, "yasuzawa": 2.9, "miyagaki": 2.9, "shimoi": 2.9, "ijuuin": 2.9, "yabiku": 2.9, "kawamukai": 2.9, "ushigome": 2.9, "izu": 2.9, "kiyonaga": 2.9, "kunimi": 2.9, "kagitani": 2.9, "kurosumi": 2.9, "tamaya": 2.9, "ankyo": 2.9, "tei": 5.4, "ariwara": 2.9, "ushimaru": 2.9, "hon-matsu": 2.9, "hareyama": 2.9, "sakagawa": 2.9, "tsuruno": 2.8, "shiihashi": 2.8, "tainaka": 2.8, "omagari": 2.8, "hata": 2.8, "juumonji": 2.8, "misonoo": 2.8, "nakaura": 2.8, "kokawa": 2.8, "nampou": 2.8, "sono": 2.8, "kakihana": 2.8, "tokito": 2.8, "namura": 2.8, "horioka": 2.8, "oka-kawa": 2.8, "shin-ryuu": 2.8, "nihonyanagi": 2.8, "ebe": 2.8, "okuzumi": 2.8, "hon": 2.8, "takatsuji": 2.8, "shirao": 2.8, "sakaba": 2.8, "minaki": 2.8, "yarita": 2.8, "numa": 2.8, "sugibayashi": 2.8, "imahori": 2.8, "chisaka": 2.8, "takiyama": 2.8, "otawara": 2.8, "kinami": 2.8, "tsubakihara": 2.8, "itonaga": 2.8, "katsuura": 2.8, "kogan": 2.8, "hakkaku": 2.8, "noshita": 2.8, "mikuriya": 2.8, "echizen": 2.8, "koganei": 2.8, "mako": 2.8, "mitsuno": 2.8, "kurumada": 2.8, "okutomi": 2.8, "kashiyama": 2.8, "oshiba": 2.8, "kitazato": 2.8, "kunishige": 2.8, "maki-shima": 2.8, "karibe": 2.8, "kurashina": 2.8, "seitan": 2.8, "inenaga": 2.8, "kamiishi": 2.8, "maeoka": 2.8, "kami-sei": 2.8, "kamisato": 2.8, "kawarada": 2.8, "kirino": 2.8, "ikeura": 2.8, "shikamata": 2.8, "ikezoe": 2.8, "hoshika": 2.8, "namikawa": 7.8, "ya-ke-saki": 2.8, "nii": 5.1, "rikimaru": 2.8, "toku-take": 2.8, "mizoi": 2.8, "nishiki": 2.8, "agawa": 2.8, "sueki": 2.8, "okude": 2.8, "daibu": 2.8, "kagiyama": 2.8, "sasazaki": 2.8, "muratani": 2.8, "hisamura": 2.8, "ouchida": 2.8, "matsumi": 2.8, "toku-mura": 2.7, "shioi": 2.7, "hisamitsu": 2.7, "shimozono": 4.9, "osabe": 2.7, "otera": 2.7, "iemura": 2.7, "nakatsubo": 2.7, "kiyomoto": 2.7, "yanagimachi": 2.7, "shirafuji": 2.7, "niwayama": 2.7, "masai": 2.7, "kuba": 2.7, "dewa": 2.7, "kada": 2.7, "iketa": 2.7, "murohashi": 2.7, "yoshimitsu": 5.2, "usukura": 2.7, "uchikura": 2.7, "isa-osamu": 2.7, "morimatsu": 2.7, "umegaki": 2.7, "kamagaya": 2.7, "shimotori": 2.7, "kaki-nai": 2.7, "enari": 2.7, "takanami": 5.1, "mada": 2.7, "ezumi": 2.7, "akisawa": 2.7, "take-no-nai": 2.7, "saku-kawa": 2.7, "masamura": 2.7, "ishio": 2.7, "namioka": 2.7, "takematsu": 2.7, "shinshutsu": 2.7, "akabira": 2.7, "juu": 2.7, "minei": 2.7, "muku-hon": 2.7, "tajika": 2.7, "kugai": 2.7, "nai": 2.7, "hirayanagi": 2.7, "shimabara": 2.7, "akima": 2.7, "agou": 2.7, "yabumoto": 2.7, "uga": 2.7, "hommiyou": 2.7, "kayahara": 2.7, "sai": 2.7, "takaha": 2.7, "akuzawa": 2.7, "nishimuta": 2.7, "konosu": 2.7, "komamura": 2.7, "arino": 2.7, "san-tsu-i": 2.7, "nambara": 2.7, "atobe": 2.7, "ho-izumi": 2.7, "nakamachi": 2.7, "mizusaki": 2.7, "banshou": 2.7, "osano": 2.7, "kuraishi": 2.7, "tokumasu": 2.7, "naemura": 2.7, "etsu": 2.7, "gotanda": 2.7, "yoshimaru": 2.7, "asako": 2.7, "kiku-shima": 2.7, "iwade": 2.7, "tsuneyoshi": 2.7, "naruoka": 2.7, "katagami": 2.7, "nagatsuma": 2.7, "sakanaka": 2.7, "kuro-miya": 2.7, "tanase": 2.7, "uraguchi": 2.7, "maeshiro": 2.7, "mizuuchi": 2.7, "mori-mi": 2.7, "okawa-nai": 2.6, "dote": 2.6, "terahara": 2.6, "mitobe": 2.6, "daimaru": 2.6, "yama": 2.6, "chikamori": 2.6, "mizumachi": 2.6, "bisai": 2.6, "shikayama": 2.6, "kusumi": 2.6, "inubushi": 2.6, "kotsutsumi": 2.6, "kinshou": 2.6, "o-shita": 2.6, "jounai": 2.6, "denda": 2.6, "koganezawa": 2.6, "toriya": 2.6, "yamakura": 2.6, "shinohe": 2.6, "fukuura": 2.6, "shinzawa": 2.6, "ichii": 2.6, "yoshikai": 2.6, "sen": 2.6, "sanshin": 2.6, "kaden": 2.6, "zou-shima": 2.6, "suzuka": 2.6, "matsuyoshi": 2.6, "kuroe": 2.6, "kamoi": 2.6, "heihou": 2.6, "sakai-ta": 2.6, "shibamoto": 2.6, "kin-shima": 2.6, "ima-saka": 2.6, "tsujiuchi": 2.6, "wakiya": 2.6, "funamizu": 2.6, "torimoto": 2.6, "otawa": 2.6, "tsukane": 2.6, "harasaki": 2.6, "imao": 2.6, "yoshihama": 2.6, "kamezawa": 2.6, "iwato": 2.6, "ikami": 2.6, "namita": 2.6, "uchihashi": 2.6, "yonemori": 2.6, "takaura": 2.6, "iikura": 2.6, "katabuchi": 2.6, "kamano": 2.6, "onaka": 2.6, "anesaki": 2.6, "hayatsu": 2.6, "komeno": 2.6, "monzen": 2.6, "atami": 2.6, "kabazawa": 2.6, "mukae": 2.6, "ishitoya": 2.6, "michibata": 2.6, "tomabechi": 2.6, "ohinata": 2.6, "toshimitsu": 2.6, "abu": 2.6, "ano": 2.5, "munemura": 2.5, "mei-kari": 2.5, "igaki": 2.5, "shoubu": 2.5, "saho": 2.5, "mae-naka": 2.5, "kizuka": 2.5, "igawa": 2.5, "douka": 2.5, "mukaihara": 2.5, "kuwae": 2.5, "kitazono": 2.5, "shimoura": 2.5, "noritake": 2.5, "masuhara": 2.5, "fukumaru": 2.5, "ueba": 2.5, "takenoshita": 2.5, "chimura": 2.5, "koiwai": 2.5, "sampei": 2.5, "tsunami": 2.5, "yagira": 2.5, "kanan": 2.5, "enjouji": 2.5, "kozakai": 2.5, "koshiyama": 2.5, "kuratomi": 2.5, "marubayashi": 2.5, "yatsuka": 2.5, "sen-tani": 2.5, "kodachi": 2.5, "ka-(kurikaesi)-bi": 2.5, "edagawa": 2.5, "sakayori": 2.5, "shigemitsu": 2.5, "ki": 2.5, "gotoda": 2.5, "tadenuma": 2.5, "tate": 2.5, "nukata": 2.5, "tsumoto": 2.5, "nada": 2.5, "kame-shima": 2.5, "korai": 2.5, "sugi-moto": 2.5, "hisama": 2.5, "shounan": 2.5, "ishinabe": 2.5, "akaogi": 2.5, "kutsuna": 2.5, "okushima": 2.5, "hisaoka": 2.5, "yamade": 2.5, "gokan": 2.5, "fujisaka": 2.5, "nakaizumi": 2.5, "ikura": 2.5, "asaba": 2.5, "wajima": 2.5, "ogo": 2.5, "eriguchi": 2.5, "shigehisa": 2.5, "nagamachi": 2.5, "yamadera": 2.5, "takuma": 2.5, "makise": 2.5, "ezoe": 2.5, "yukimura": 2.5, "onoyama": 2.5, "takeshi-shima": 2.5, "agata": 2.5, "kotaka": 2.5, "tairin": 2.5, "shimoyamada": 2.5, "nagafuchi": 2.5, "kawami": 2.5, "yoza": 2.5, "fukue": 2.5, "sakisaka": 2.5, "shiratori": 2.5, "yahara": 2.5, "chigusa": 2.5, "kanetsu": 2.5, "genda": 2.5, "abukawa": 2.5, "tsunemi": 2.5, "sugiuchi": 2.5, "oyake": 2.5, "kanezuka": 2.5, "chimei": 2.5, "kumakiri": 2.5, "yomoda": 2.5, "o-naka": 2.4, "sakaya": 2.4, "horiki": 2.4, "yamakata": 2.4, "amimoto": 2.4, "yoneoka": 2.4, "komachi": 2.4, "saisho": 2.4, "koezuka": 2.4, "mimuro": 2.4, "hiraguri": 2.4, "kayu-kawa": 2.4, "kitamori": 2.4, "makuta": 2.4, "okauchi": 2.4, "dejima": 2.4, "ishiuchi": 2.4, "tsuruga": 2.4, "naritomi": 2.4, "gushi": 2.4, "uchihara": 2.4, "onimaru": 2.4, "tsunoyama": 2.4, "oeda": 2.4, "mayama": 4.6, "mibu": 2.4, "indou": 2.4, "shimotsu": 2.4, "yogo": 2.4, "asama": 2.4, "teshirogi": 2.4, "ushiki": 2.4, "kitsunai": 2.4, "gyokushu": 2.4, "iisaka": 2.4, "moritaka": 2.4, "ideue": 2.4, "kuniya": 2.4, "terazono": 2.4, "hirohashi": 2.4, "muranishi": 2.4, "enokihara": 2.4, "koshimura": 2.4, "tanabu": 2.4, "yagihara": 2.4, "sakumoto": 2.4, "kishi-shita": 2.4, "fukasaku": 2.4, "taikai": 2.4, "isokawa": 2.4, "funai": 2.4, "nishisato": 2.4, "izumisawa": 2.4, "jahana": 2.4, "shiroya": 2.4, "ishigooka": 2.4, "kadoya": 2.4, "hatamoto": 2.4, "hagino-tani": 2.4, "nagayasu": 4.699999999999999, "maesaki": 2.4, "wakamoto": 2.4, "nanao": 2.4, "toyomura": 2.4, "sakimura": 2.4, "matsunaka": 2.4, "sumitani": 2.4, "hiraguchi": 2.4, "sagou": 2.4, "kurai": 2.4, "shibue": 2.4, "kayou": 2.4, "hiraizumi": 2.4, "roppongi": 2.4, "koishikawa": 2.4, "kiyooka": 2.4, "okahashi": 2.4, "suiryuu": 2.4, "fujine": 2.4, "tsukayama": 2.4, "kaieda": 2.4, "kutsukake": 2.4, "aratake": 2.4, "hirashita": 2.4, "taruya": 2.4, "kidoguchi": 2.4, "kamimoto": 2.4, "nishikata": 2.4, "osafune": 2.4, "itoda": 2.4, "to-mei": 2.4, "negami": 2.4, "gibo": 2.4, "tamaoka": 2.4, "chuto": 2.4, "iwatachi": 2.3, "kitsu": 2.3, "maesato": 2.3, "hosogoe": 2.3, "oshitani": 2.3, "shigeyama": 2.3, "kiri-ta": 2.3, "tawa": 2.3, "daikichi": 2.3, "kanegusuku": 2.3, "taikoku": 2.3, "hisaki": 2.3, "tokoyoda": 2.3, "hondou": 4.6, "tsumori": 2.3, "zukeran": 2.3, "komatsuda": 2.3, "orii": 2.3, "hombu": 2.3, "isami": 2.3, "matsuie": 2.3, "shibuki": 2.3, "matsunuma": 2.3, "nanami": 2.3, "shirako": 2.3, "tokioka": 2.3, "kurano": 2.3, "kachi-shima": 2.3, "kokkou": 2.3, "izumimoto": 2.3, "sata": 2.3, "amagasa": 2.3, "yasutani-ya": 2.3, "nakaishi": 2.3, "nakamuta": 2.3, "washizu": 4.5, "futakuchi": 2.3, "kumashiro": 2.3, "satonaka": 2.3, "sone-hara": 2.3, "konagai": 2.3, "kikunaga": 2.3, "sanji": 2.3, "dobugawa": 2.3, "fukuyoshi": 2.3, "okinaka": 2.3, "misumi": 2.3, "ishima": 2.3, "nukui": 2.3, "shutsu-saki": 2.3, "katsui": 2.3, "tanaami": 2.3, "onoguchi": 2.3, "shou-sono": 2.3, "wakata": 2.3, "ukon": 2.3, "shimonaka": 2.3, "amuro": 2.3, "inoda": 2.3, "tokuhiro": 2.3, "kirimura": 2.3, "nakaniwa": 2.3, "maru-shima": 2.3, "kurioka": 2.3, "hisasue": 2.3, "hirasaki": 2.3, "kome-maru": 2.3, "hisakawa": 2.3, "noyama": 2.3, "tsurumaru": 2.3, "nomi": 2.3, "kihira": 2.3, "makiguchi": 2.3, "ato": 2.3, "mukaigawa": 2.3, "tabuse": 2.3, "yumura": 2.3, "katogi": 2.3, "karashima": 2.3, "shoken": 2.3, "oke-ta": 2.3, "hashitani": 2.3, "kasagawa": 2.3, "segami": 2.3, "akitaya": 2.3, "nikyou": 2.3, "mobara": 2.3, "shiwaku": 2.3, "shichinohe": 2.3, "asamizu": 2.3, "shinozawa": 2.3, "shio": 2.3, "shiojiri": 2.3, "yoshikura": 2.2, "kamigaki": 2.2, "konnai": 2.2, "oda-kura": 2.2, "amaki": 2.2, "hatai": 2.2, "nishimatsu": 2.2, "utsuki": 2.2, "harai": 2.2, "morisumi": 2.2, "shionoya": 2.2, "igusa": 2.2, "mannaka": 2.2, "ya-tani": 2.2, "takemae": 2.2, "tsurukawa": 2.2, "okuwaki": 2.2, "kibayashi": 2.2, "tomimoto": 2.2, "fuji-saki": 2.2, "tano-kura": 2.2, "ya-ke-bu": 2.2, "mikoshiba": 2.2, "kajima": 2.2, "tokie": 2.2, "ishiki": 2.2, "karakama": 2.2, "uto": 2.2, "rikitake": 2.2, "sakatani": 2.2, "gojuu-hatake": 2.2, "shinei-ta": 2.2, "zaizen": 2.2, "kabe": 2.2, "tsuboyama": 2.2, "fu-hayashi": 2.2, "hanami": 2.2, "iwatsubo": 2.2, "aizu": 2.2, "jougo": 2.2, "kuru": 2.2, "kakimi": 2.2, "ebi": 2.2, "kadogawa": 2.2, "sassa-no": 2.2, "aeba": 2.2, "sukigara": 2.2, "takarabe": 2.2, "katsuhara": 2.2, "tomiya": 2.2, "kitakaze": 2.2, "taira-ken": 2.2, "haseyama": 2.2, "sekioka": 2.2, "ayusawa": 2.2, "toyosato": 2.2, "yoshitomo": 2.2, "tango": 2.2, "iwadate": 2.2, "togame": 2.2, "shumpon": 2.2, "kugou": 2.2, "nagatsu": 2.2, "jinushi": 2.2, "morie": 2.2, "kumekawa": 2.2, "kenjou": 2.2, "konda": 2.2, "sakama": 2.2, "namihei": 2.2, "murasugi": 2.2, "tsukuta": 2.2, "koganemaru": 2.2, "ogasa": 2.2, "gesu": 2.2, "sakaeda": 2.2, "fukuno": 2.2, "achiwa": 2.2, "hoshitani": 2.2, "i-no-ue": 2.2, "nunomura": 2.2, "ashiya": 2.2, "hinohara": 2.2, "nagasue": 2.2, "sueyasu": 2.2, "tsujiguchi": 2.2, "hato": 2.2, "miike": 2.2, "kuri-shima": 2.2, "shimokawara": 2.2, "kaki-shima": 2.2, "santo": 2.2, "sanguu": 2.2, "ganeko": 2.2, "tochihara": 2.2, "ritsu-saki": 2.2, "yabu-saki": 2.2, "watakabe": 2.2, "iden": 2.2, "niwata": 2.2, "saka-tsume": 2.2, "kaneshige": 2.2, "hiromatsu": 2.2, "katagai": 2.2, "oji": 2.2, "hasuda": 2.2, "inatsu": 2.2, "okusaki": 2.2, "sambu": 2.2, "funabiki": 2.2, "kishibe": 2.2, "ohisa": 2.2, "sanuki": 2.2, "shikama": 2.2, "iwamuro": 2.2, "kagimoto": 2.2, "hoshihara": 2.2, "taka-ba": 2.2, "tomoe": 2.2, "nabekura": 2.2, "hiroshige": 2.2, "hayamizu": 2.2, "futamata": 2.1, "kurotani": 2.1, "machi": 2.1, "iimuro": 2.1, "shimo": 2.1, "yutani": 2.1, "mizushiro": 2.1, "aze-chi": 2.1, "ken": 2.1, "kawano-hen": 2.1, "fuchita": 2.1, "arayama": 2.1, "amachi": 2.1, "okanishi": 2.1, "tatemichi": 2.1, "shiiya": 2.1, "maya": 2.1, "iwakoshi": 2.1, "fukuo": 2.1, "katsunuma": 2.1, "koka": 2.1, "haruta": 2.1, "okagami": 2.1, "nirasawa": 2.1, "gyou-takeshi": 2.1, "idogawa": 2.1}
    return float(gJapaneseLastName.get(word.lower(), '0.0'))

def popularity_as_korean_lastname(word):
    # https://en.wikipedia.org/wiki/List_of_Korean_surnames
    gKoreanLastName = {"ga":9950,"gan":2525,"gal":2086,"gam":6024,"gang":1269,"gyeon":1684,"gyeong":116958,"gye":6641,"go":471429,"gok":101,"gong":92340,"gwak":203365,"gwan":20,"gyo":26,"gu":208550,"guk":20768,"gung":572,"gwok":183,"gwon":706212,"geun":170,"geum":25472,"gi":29062,"gil":38173,"gim":10689,"na":161015,"ra":161015,"nan":8,"ran":10,"nam":275659,"namgung":21313,"nang":181,"rang":181,"nae":374,"no":315372,"ro":315372,"noe":19,"roe":19,"da":7,"dan":1632,"dam":47,"dang":1146,"dae":669,"do":57946,"dokgo":502,"don":117,"dong":5936,"dongbang":180,"du":6428,"deung":20,"deungjeong":5,"ryeo":80672,"roh":67,"ryu":963498,"ree":240,"rim":1015,"ma":39196,"man":124,"mangjeol":8,"mae":201,"maeng":22028,"myeong":29110,"mo":21912,"mok":8859,"myo":21,"moo":15,"moobon":6,"muk":172,"mun":464047,"mi":16,"min":171799,"bak":4192,"ban":28223,"bang":129559,"bae":400669,"baek":382447,"beon":6,"beom":3838,"byeon":138802,"bo":16,"bok":9538,"bokho":5,"bong":12959,"bu":10604,"bi":16,"bin":5782,"bing":763,"buyeo":89,"sa":10998,"sagong":4488,"san":9,"sam":38,"sang":2416,"seo":752233,"seomun":2044,"seok":60607,"seon":42842,"seonu":3648,"seol":45692,"seob":75,"seong":199160,"so":53856,"son":457356,"song":683521,"su":47,"sun":1237,"seung":3430,"si":4354,"sin":986001,"sim":272049,"a":529,"an":685688,"ae":24,"ya":77,"yang":530554,"ryang":530554,"eo":18929,"eogeum":8,"eom":144660,"yeo":80672,"yeon":34850,"ryeon":34850,"yeom":69428,"ryeom":69428,"yeop":571,"yeong":24,"ye":13587,"o":763334,"ok":25107,"on":5418,"ong":967,"wan":6,"wang":25581,"yo":29,"yong":15276,"ryong":15276,"u":195729,"un":118,"won":130174,"wi":32191,"yu":963498,"yuk":23455,"ryuk":23455,"yun":1020,"eun":16927,"eum":5604,"i":7307,"ri":7307,"in":22363,"im":1015,"ja":75,"jang":1021,"jeon":749266,"jeom":158,"jeong":2407,"je":21988,"jegal":5735,"jo":1453,"jong":681,"jwa":3383,"ju":232063,"jeung":18,"ji":160147,"jin":186310,"cha":194788,"chang":1095,"chae":131757,"cheon":121927,"cho":1453,"choe":2340,"chu":232063,"tak":21099,"tan":1632,"tae":9073,"pan":28223,"paeng":2935,"pyeon":16689,"pyeong":515,"po":57,"pyo":30749,"pung":651,"pi":6578,"pil":174,"ha":233106,"hak":35,"han":773537,"ham":80659,"hae":155,"heo":326782,"hyeon":88831,"hyeong":7328,"ho":5853,"hong":558994,"hwa":915,"hwang":697475,"hwangmok":5,"hwangbo":10427,"hu":74,"ka":9950,"kan":2525,"kal":2086,"kam":6024,"kang":1269,"kye":6641,"ko":471429,"kok":101,"kong":92340,"kwak":203365,"kwan":20,"kyo":26,"ku":208550,"kuk":20768,"kung":572,"ki":29062,"kil":38173,"kim":10689,"ta":7,"tam":47,"tang":1146,"to":57946,"tokko":502,"ton":117,"tong":5936,"tongbang":180,"tu":6428,"rah":25974,"rahn":10,"roi":19,"pak":4192,"pang":129559,"pae":400669,"paek":382447,"bun":6,"pok":9538,"pokho":5,"pong":12959,"pu":10604,"pin":5782,"ping":763,"sub":75,"yi":7307,"che":21988,"chegal":5735,"chong":681,"chwa":3383,"chi":160147,"chin":186310}
    return float(gKoreanLastName.get(word.lower(), '0.0'))


def matching_author_string(origName):
    from nameparser import HumanName

    tag = 'unknown'
    # try:
    tag = 'ascii'
    words = origName.replace("'", '').replace('-', '').replace(',', '').replace('.', '').split()
    for i in reversed(range(len(words))):
        # Upper letter
        if len(words[i]) == 2 and words[i] == words[i].upper():
            words[i] = words[i][0] + ' ' + words[i][1]
        # word in ()
        if len(words[i]) > 2 and words[i][0] == '(' and words[i][-1] == ')':
            del words[i]
    result = ' '.join(words)
    if len(words) == 2:
        # adjust japanese sort
        if len(words[0]) > 1 and popularity_as_japanese_lastname(words[0]) > 0:
            if popularity_as_japanese_lastname(words[0]) > popularity_as_japanese_lastname(words[1]):
                result = words[1] + ' ' + words[0]
                tag = 'japanese-english'
        # adjust chinese sort
        elif len(words[0]) > 1 and popularity_as_chinese_lastname(words[0]) > 0:
            if popularity_as_chinese_lastname(words[0]) > popularity_as_chinese_lastname(words[1]):
                result = words[1] + ' ' + words[0]
                tag = 'chinese-english'
        # adjust korean sort
        elif len(words[0]) > 1 and popularity_as_korean_lastname(words[0]) > 0:
            if popularity_as_korean_lastname(words[0]) > popularity_as_korean_lastname(words[1]):
                result = words[1] + ' ' + words[0]
                tag = 'korean-english'
        # 1 letter in second
        elif len(words[1]) == 1:
            result = words[1] + ' ' + words[0]
            tag = '1-letter-in-second'
        # # 2 letters in second: Ea
        # elif len(words[1]) == 2 and words[1][0] == words[1][0].upper() and words[1][1] == words[1][1].lower():
        #     result = words[1][0] + ' ' + words[1][1] + ' ' + words[0]
        #     tag = '2-letters-in-second'

    elif len(words) == 3:
        # 1 letter in second and third
        if len(words[1]) == 1 and len(words[2]) == 1:
            result = words[1] + ' ' + words[2] + ' ' + words[0]
    normalized_string = result.lower()

    response = normalized_string
    try:
        pass
        parsed_name = HumanName(normalized_string)
        first = parsed_name.first
        last = parsed_name.last

        first_initial = u""
        if first:
            first_initial = first[0]
            response = u"{};{}".format(last.decode("utf-8"), first_initial.decode("utf-8"))

    except:
    # except UnicodeEncodeError:
        pass


    return response

