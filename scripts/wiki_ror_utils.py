import re
import random

from requests_cache import CachedSession, RedisCache
from redis import Redis

from app import REDIS_URL

connection = Redis.from_url(REDIS_URL)


def cached_session():
    cache_backend = RedisCache(connection=connection, expire_after=None)
    random_expire_one_to_three_days = random.randint(86400, 259200)
    session = CachedSession(cache_name="cache", backend=cache_backend, expire_after=random_expire_one_to_three_days)
    return session


def find_wikidata_id(display_name, words_to_ignore=None, words_to_include=None):
    # search the wikidata API for the display_name
    session = cached_session()
    r = session.get(
        f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={display_name}&language=en&format=json"
    )
    if r.status_code == 200:
        response = r.json()
        if len(response["search"]) == 1:
            # check to ensure description is not scientific article
            search = response.get("search", None)
            if search and len(search) > 0:
                description = search[0].get("description", None)
                if description and words_to_ignore and words_to_ignore in description.lower():
                    return None
                elif description and words_to_include and words_to_include not in description.lower():
                    return None
            wikidata_shortcode = response["search"][0]["id"]
            wikidata_id = f"https://www.wikidata.org/entity/{wikidata_shortcode}"
            return wikidata_id


def normalize_wikidata_id(wikidata):
    if not wikidata:
        return None
    wikidata = wikidata.strip().upper()
    p = re.compile(r"Q\d*")
    matches = re.findall(p, wikidata)
    if len(matches) == 0:
        return None
    wikidata = matches[0]
    wikidata = wikidata.replace("\0", "")
    return wikidata


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


def fetch_wikidata_response(wikidata_shortcode):
    wikidata_url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={wikidata_shortcode}&format=json"
    session = cached_session()
    response = session.get(wikidata_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def fetch_ror_response(ror_id):
    ror_shortcode = normalize_ror(ror_id)
    ror_url = f"https://api.ror.org/organizations/{ror_shortcode}"
    session = cached_session()
    response = session.get(ror_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def fetch_crossref_response(uri):
    session = cached_session()
    response = session.get(uri)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def get_homepage_url_from_wikidata(wikidata_response, wikidata_shortcode):
    wikidata_homepage_url = None
    if wikidata_response:
        try:
            claims = wikidata_response["entities"][wikidata_shortcode]["claims"]
            if "P856" in claims:
                wikidata_homepage_url = claims["P856"][0]["mainsnak"]["datavalue"]["value"]
        except KeyError:
            print(f"Error getting homepage url from wikidata for {wikidata_shortcode}")
    return wikidata_homepage_url


def get_homeage_url_from_ror(ror_response):
    ror_homepage_url = None
    if ror_response:
        if "links" in ror_response and len(ror_response["links"]) > 0:
            ror_homepage_url = ror_response["links"][0]
    return ror_homepage_url


def get_homepage_url(wikidata_id, ror_id):
    wikidata_homepage_url = None
    ror_homepage_url = None

    if wikidata_id:
        wikidata_shortcode = normalize_wikidata_id(wikidata_id)
        wikidata_response = fetch_wikidata_response(wikidata_shortcode)
        wikidata_homepage_url = get_homepage_url_from_wikidata(
            wikidata_response, wikidata_shortcode
        )
    if ror_id:
        ror_shortcode = normalize_ror(ror_id)
        ror_response = fetch_ror_response(ror_shortcode)
        ror_homepage_url = get_homeage_url_from_ror(ror_response)

    # strip trailing slash
    if wikidata_homepage_url:
        wikidata_homepage_url = wikidata_homepage_url.rstrip("/")
    if ror_homepage_url:
        ror_homepage_url = ror_homepage_url.rstrip("/")

    preferred_url = wikidata_homepage_url or ror_homepage_url

    # check if the only difference is http vs https, if so use https version
    if preferred_url and preferred_url.startswith("http://"):
        https_url = preferred_url.replace("http://", "https://")
        if https_url in [wikidata_homepage_url, ror_homepage_url]:
            preferred_url = https_url
    return preferred_url


def get_description(wikidata_id):
    wikidata_description = None

    if wikidata_id:
        wikidata_shortcode = normalize_wikidata_id(wikidata_id)
        wikidata_response = fetch_wikidata_response(wikidata_shortcode)
        if wikidata_response:
            wikidata_description = (
                wikidata_response["entities"][wikidata_shortcode]
                .get("descriptions", {})
                .get("en", {})
                .get("value", None)
            )
    return wikidata_description


def get_country_code(wikidata_id, ror_id):
    wikidata_country_code = None
    ror_country_code = None

    if wikidata_id:
        wikidata_shortcode = normalize_wikidata_id(wikidata_id)
        wikidata_response = fetch_wikidata_response(wikidata_shortcode)
        if wikidata_response:
            try:
                claims = wikidata_response["entities"][wikidata_shortcode]["claims"]
                if "P17" in claims:
                    wikidata_country_shortcode = claims["P17"][0]["mainsnak"]["datavalue"][
                        "value"
                    ]["id"]
                    wikidata_country_response = fetch_wikidata_response(
                        wikidata_country_shortcode
                    )
                    if wikidata_country_response:
                        wikidata_country_code = (
                            wikidata_country_response["entities"][wikidata_country_shortcode]
                            .get("claims", {})
                            .get("P297", [{}])[0]
                            .get("mainsnak", {})
                            .get("datavalue", {})
                            .get("value", None)
                        )
            except KeyError:
                print(f"Error getting country code for {wikidata_shortcode}")
    if ror_id:
        ror_shortcode = normalize_ror(ror_id)
        ror_response = fetch_ror_response(ror_shortcode)
        if ror_response:
            ror_country_code = ror_response.get("country", {}).get("country_code", None)

    return wikidata_country_code or ror_country_code


def get_image_url(wikidata_id):
    wikidata_image_url = None
    if wikidata_id:
        wikidata_shortcode = normalize_wikidata_id(wikidata_id)
        wikidata_response = fetch_wikidata_response(wikidata_shortcode)
        if wikidata_response:
            # try to get the logo first
            claims = wikidata_response["entities"][wikidata_shortcode]["claims"]
            if "P154" in claims:
                try:
                    wikidata_image_file = claims["P154"][0]["mainsnak"]["datavalue"][
                        "value"
                    ]
                    wikidata_image_url = f"https://commons.wikimedia.org/w/index.php?title=Special:Redirect/file/{wikidata_image_file}"
                except KeyError:
                    print(f"Error getting image for {wikidata_shortcode}")

            # if no logo, try to get an image
            elif "P18" in claims:
                try:
                    wikidata_image_file = claims["P18"][0]["mainsnak"]["datavalue"]["value"]
                    wikidata_image_url = f"https://commons.wikimedia.org/w/index.php?title=Special:Redirect/file/{wikidata_image_file}"
                except KeyError:
                    print(f"Error getting image for {wikidata_shortcode}")
    return wikidata_image_url


def get_image_thumbnail_url(image_url):
    if image_url:
        return f"{image_url}&width=300"
