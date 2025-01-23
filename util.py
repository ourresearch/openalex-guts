import datetime
import time
import unicodedata

import shortuuid
import sqlalchemy
import logging
import math
import bisect
import urllib.parse
import re
import os
import collections
import requests
import hashlib
import heroku3
import json
import copy
from nameparser import HumanName
import string

from tenacity import retry, stop_after_attempt, wait_exponential
from unidecode import unidecode
from sqlalchemy import sql, text
from sqlalchemy import exc
from subprocess import call
from requests.adapters import HTTPAdapter
import csv
from langdetect import detect_langs, DetectorFactory, LangDetectException

from app import unpaywall_db_engine, db

UNPAYWALL_DB_CONN = None

def fetch_top_1k_titles():
    result = db.session.execute('SELECT unpaywall_normalize_title FROM top_1000_titles;').fetchall()
    return {row[0] for row in result}

TOP_TITLES = fetch_top_1k_titles()


def entity_md5(entity_repr):
    if isinstance(entity_repr, int):
        return text_md5(str(entity_repr))
    if isinstance(entity_repr, dict):
        entity_copy = entity_repr.copy()
        entity_copy.pop("updated_date", None)
        entity_copy.pop("updated", None)
        entity_copy.pop("@timestamp", None)
        entity_str = json.dumps(entity_copy, sort_keys=True)
        return text_md5(entity_str)


def text_md5(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def struct_changed(before, after):
    if (before is None) != (after is None):
        return True

    before_json = json.dumps(before, sort_keys=True)
    after_json = json.dumps(after, sort_keys=True)

    return before_json != after_json


class NoDoiException(Exception):
    pass


class NotJournalArticleException(Exception):
    pass


class DelayedAdapter(HTTPAdapter):
    def send(
            self, request, stream=False, timeout=None, verify=True, cert=None,
            proxies=None
    ):
        # logger.info(u"in DelayedAdapter getting {}, sleeping for 2 seconds".format(request.url))
        # sleep(2)
        start_time = time.time()
        response = super(DelayedAdapter, self).send(
            request, stream, timeout, verify, cert, proxies
        )
        # logger.info(u"   HTTPAdapter.send for {} took {} seconds".format(request.url, elapsed(start_time, 2)))
        return response


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
        resp[k] = round(float(v) / total, 2)
    return resp


def calculate_percentile(refset, value):
    if value is None:  # distinguish between that and zero
        return None

    matching_index = bisect.bisect_left(refset, value)
    percentile = float(matching_index) / len(refset)
    # print u"percentile for {} is {}".format(value, percentile)

    return percentile


def clean_html(raw_html):
    cleanr = re.compile("<.*?>")
    try:
        cleantext = re.sub(cleanr, "", raw_html)
    except TypeError:
        cleantext = raw_html
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
    doi = replace_doi_bad_chars(doi)

    # test cases for this regex are at https://regex101.com/r/zS4hA0/4
    p = re.compile(r"(10\.\d+/[^\s]+)")
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

    return doi.replace("\0", "")


def normalize_orcid(orcid):
    if not orcid:
        return None
    orcid = orcid.strip().upper()
    p = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])")
    matches = re.findall(p, orcid)
    if len(matches) == 0:
        return None
    orcid = matches[0]
    orcid = orcid.replace("\0", "")
    return orcid


def normalize_pmid(pmid):
    if not pmid:
        return None
    pmid = pmid.strip().lower()
    p = re.compile("(\d+)")
    matches = re.findall(p, pmid)
    if len(matches) == 0:
        return None
    pmid = matches[0]
    pmid = pmid.replace("\0", "")
    return pmid


def normalize_ror(ror):
    if not ror:
        return None
    ror = ror.strip().lower()
    p = re.compile(r"([a-z\d]*$)")
    matches = re.findall(p, ror)
    if len(matches) == 0:
        return None
    ror = matches[0]
    ror = ror.replace("\0", "")
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
    issn = issn.replace("\0", "")
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
    wikidata = wikidata.replace("\0", "")
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
    p = re.compile("([WAICS]\d{2,})")
    matches = re.findall(p, openalex_id)
    if len(matches) == 0:
        return None
    clean_openalex_id = matches[0]
    clean_openalex_id = clean_openalex_id.replace("\0", "")
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
        no_punc = "".join(
            e for e in input_string if (e.isalnum() or e.isspace()))
    return no_punc


# from http://stackoverflow.com/a/11066579/596939
def replace_punctuation(text, sub):
    punctutation_cats = set(["Pc", "Pd", "Ps", "Pe", "Pi", "Pf", "Po"])
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
    raise TypeError("Type not serializable")


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

    short_number = "{}".format(round(divided, 2))[:-1]
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


def replace_doi_bad_chars(doi):
    replace_chars = {"‐": "-"}
    for char, replacement in replace_chars.items():
        doi = doi.replace(char, replacement)
    return doi


def clean_doi(dirty_doi, return_none_if_error=False):
    if not dirty_doi:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no DOI at all.")

    dirty_doi = dirty_doi.strip()
    dirty_doi = dirty_doi.lower()
    dirty_doi = replace_doi_bad_chars(dirty_doi)

    # test cases for this regex are at https://regex101.com/r/zS4hA0/1
    p = re.compile(r"(10\.\d+\/[^\s]+)")

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
    resp = resp.replace('"', "")

    # remove trailing period, comma -- it is likely from a sentence or citation
    if resp.endswith(",") or resp.endswith("."):
        resp = resp[:-1]

    return resp


def pick_best_url(urls):
    if not urls:
        return None

    # get a backup
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
    if len(my_list) % 2 == 1:
        return my_list[((len(my_list) + 1) / 2) - 1]
    if len(my_list) % 2 == 0:
        return (
                float(sum(my_list[(len(my_list) / 2) - 1: (
                                                                  len(my_list) / 2) + 1])) / 2.0
        )


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
        yield l[i: i + n]


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


def truncate_on_word_break(string, max_length):
    if not string:
        return string

    if len(string) <= max_length:
        return string

    break_index = 0
    for word_break in re.finditer(r"\b", string):
        if word_break.start() < max_length:
            break_index = word_break.start()
        else:
            break

    if break_index:
        return string[0:break_index] + "…"
    else:
        return string[0: max_length - 1] + "…"


def str_to_bool(x):
    if x.lower() in ["true", "1", "yes"]:
        return True
    elif x.lower() in ["false", "0", "no"]:
        return False
    else:
        raise ValueError("This string can't be cast to a boolean.")


# from http://stackoverflow.com/a/20007730/226013
ordinal = lambda n: "%d%s" % (
    n,
    "tsnrhtdd"[(n / 10 % 10 != 1) * (n % 10 < 4) * n % 10:: 4],
)


# from http://farmdev.com/talks/unicode/
def to_unicode_or_bust(obj, encoding="utf-8"):
    if isinstance(obj, str):
        if not isinstance(obj, str):
            obj = str(obj, encoding)
    return obj


def remove_nonprinting_characters(input, encoding="utf-8"):
    input_was_unicode = True
    if isinstance(input, str):
        if not isinstance(input, str):
            input_was_unicode = False

    unicode_input = to_unicode_or_bust(input)

    # see http://www.fileformat.info/info/unicode/category/index.htm
    char_classes_to_remove = ["C", "M", "Z"]

    response = "".join(
        c
        for c in unicode_input
        if unicodedata.category(c)[0] not in char_classes_to_remove
    )

    if not input_was_unicode:
        response = response.encode(encoding)

    return response


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


def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    raise TypeError(repr(o) + " is not JSON serializable")


# like the one below but similar to what we used in redshift
def normalize_title_like_sql(title, remove_stop_words=True):
    import re

    response = title

    if not response:
        return ""

    # just first n characters
    response = response[0:500]

    # lowercase
    response = response.lower()

    # has to be before remove_punctuation
    # the kind in titles are simple <i> etc, so this is simple
    response = re.sub("<.*?>", "", response)

    # remove articles and common prepositions
    if remove_stop_words:
        response = re.sub(
            r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", response
        )

    # remove everything except alphas
    response = "".join(e for e in response if (e.isalpha()))

    return response


# matches sql/f_generate_inverted_index.sql python user defined function in redshift
def f_generate_inverted_index(abstract_string):
    import re
    import json
    from collections import OrderedDict

    # remove jat tags and unnecessary tags and problematic white space
    abstract_string = re.sub("\b", " ", abstract_string)
    abstract_string = re.sub("\n", " ", abstract_string)
    abstract_string = re.sub("\t", " ", abstract_string)
    abstract_string = re.sub("<jats:[^<]+>", " ", abstract_string)
    abstract_string = re.sub("</jats:[^<]+>", " ", abstract_string)
    abstract_string = re.sub("<p>", " ", abstract_string)
    abstract_string = re.sub("</p>", " ", abstract_string)
    abstract_string = " ".join(re.split("\s+", abstract_string))

    # build inverted index
    invertedIndex = OrderedDict()
    words = abstract_string.split()
    for i in range(len(words)):
        if words[i] not in invertedIndex:
            invertedIndex[words[i]] = []
        invertedIndex[words[i]].append(i)
    result = {
        "IndexLength": len(words),
        "InvertedIndex": invertedIndex,
    }

    return json.dumps(result, ensure_ascii=False)


def matching_author_strings(author):
    author = remove_latin_characters(author)
    author = remove_author_prefixes(author)
    author_name = HumanName(author)

    first_name = clean_author_name(author_name.first)
    last_name = clean_author_name(author_name.last)
    first_initial = first_name[0] if first_name else ""
    return [
        f"{last_name};{first_initial}",
        f"{first_name};{last_name}",
        f"{last_name};{first_name}",
    ]


def remove_latin_characters(author):
    if any("\u0080" <= c <= "\u02AF" for c in author):
        author = (
            unicodedata.normalize("NFKD", author)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    return author


def remove_author_prefixes(author):
    prefixes = ["None ", "Array "]
    for prefix in prefixes:
        if author.startswith(prefix):
            author = author.replace(prefix, "")
    return author


def clean_author_name(author_name):
    return re.sub(r"[ \-‐.'’ ́>]", "", author_name).lower().strip()


def work_has_null_author_ids(w):
    if isinstance(w, dict):
        author_ids = list(
            {a.get("author", {}).get("id") for a in w.get("authorships", [])}
        )
        return any(not author_id for author_id in author_ids)
    # work object, use relationships
    author_ids = list({a.author_id for a in w.affiliations})
    return any(not author_id for author_id in author_ids)


def majority_ascii(s, threshold=0.5):
    return sum([char.isascii() for char in s]) / len(s) > threshold


def majority_uppercase(s, threshold=0.5):
    return sum([char.isupper() for char in s]) / len(s) > threshold


def punctuation_density(words):
    return sum(
        [word[-1] in string.punctuation for word in words if word]) / len(words)


def words_within_distance(text, word1, word2, distance):
    words = text.split()
    word1_indices = [i for i, w in enumerate(words) if w == word1]
    word2_indices = [i for i, w in enumerate(words) if w == word2]

    for i in word1_indices:
        for j in word2_indices:
            if abs(i - j) <= distance:
                return True
    return False


def detect_language_abstract(
        abstract_words, probability_threshold=0.7,
        punctuation_density_threshold=0.5
):
    DetectorFactory.seed = 0
    if abstract_words:
        abstract = " ".join(abstract_words)
        if majority_uppercase(abstract):
            # langdetect does poorly if input is in all caps
            abstract = abstract.lower()
        abstract_language = detect_langs(abstract)
        if abstract_language and abstract_language[0]:
            if (
                    (len(abstract) > 20 or not majority_ascii(abstract))
                    and abstract_language[0].prob >= probability_threshold
                    and punctuation_density(
                abstract_words) < punctuation_density_threshold
            ):
                return abstract_language
    return None


def detect_language_title(
        title, probability_threshold=0.7, punctuation_density_threshold=0.5
):
    DetectorFactory.seed = 0
    if title:
        if majority_uppercase(title):
            # langdetect does poorly if input is in all caps
            title = title.lower()
        title_language = detect_langs(title)
        if title_language and title_language[0]:
            if (len(title) > 15 or not majority_ascii(title)) and (
                    title_language[0].prob >= probability_threshold
            ):
                return title_language
    return None


def detect_language_from_abstract_and_title(
        abstract_words, title, probability_threshold=0.7,
        punctuation_density_threshold=0.5
):
    # use some heuristics to catch some edge cases:
    # - detect language from abstract if probability is high and abstract isn't too short (for abstracts with ascii/latin characters)
    #   - (also check for punctuation density in abstract, to catch cases where the abstract is a punctuation-separated list of author names)
    # - otherwise try title, with similar rules
    # - give up and return None if neither of those work
    DetectorFactory.seed = 0

    try:
        if abstract_words:
            abstract_language = detect_language_abstract(
                abstract_words, probability_threshold,
                punctuation_density_threshold
            )
            if abstract_language and abstract_language[0]:
                return abstract_language[0].lang

        if title:
            title_language = detect_language_title(
                title, probability_threshold, punctuation_density_threshold
            )
            if title_language and title_language[0]:
                return title_language[0].lang
    except LangDetectException:
        pass

    return None


def get_crossref_json_from_unpaywall(doi: str):
    global UNPAYWALL_DB_CONN
    if not UNPAYWALL_DB_CONN:
        UNPAYWALL_DB_CONN = unpaywall_db_engine.connect()
    rows = UNPAYWALL_DB_CONN.execute(
        text('SELECT crossref_api_raw_new FROM pub WHERE id = :doi'),
        {'doi': doi}).fetchall()
    if not rows:
        return None
    return rows[0][0]


def print_openalex_error(retry_state):
    if retry_state.outcome.failed:
        print(
            f'[!] Error making OpenAlex API call (attempt #{retry_state.attempt_number}): {retry_state.outcome.exception()}')


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=4, max=256),
       retry_error_callback=print_openalex_error)
def get_openalex_json(url, params, s=None):
    if not s:
        s = requests
    r = s.get(url, params=params,
              verify=False)
    r.raise_for_status()
    return r.json()


def openalex_works_paginate(oax_filter, select=None):
    params = {'mailto': 'team@ourresearch.org',
              'filter': oax_filter,
              'per-page': '200',
              'cursor': '*',
              'bypass_cache': 'true'}
    if select:
        params['select'] = select
    s = requests.session()
    while True:
        j = get_openalex_json('https://api.openalex.org/works', params, s)
        page = j['results']
        if next_cursor := j['meta'].get('next_cursor'):
            params['cursor'] = next_cursor
        else:
            break
        if not page:
            break
        yield page

def make_recordthresher_id():
    return shortuuid.uuid()[:22]
