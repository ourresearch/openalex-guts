from scripts.wikidata_ror.normalize import normalize_wikidata_id, normalize_ror
from scripts.wikidata_ror.response import fetch_wikidata_response, fetch_ror_response


def get_homepage_url(wikidata_id, ror_id=None):
    wikidata_homepage_url = None
    ror_homepage_url = None

    if wikidata_id:
        wikidata_shortcode = normalize_wikidata_id(wikidata_id)
        wikidata_response = fetch_wikidata_response(wikidata_shortcode)
        wikidata_homepage_url = get_homepage_url_from_wikidata(wikidata_response, wikidata_shortcode)

    if ror_id:
        ror_shortcode = normalize_ror(ror_id)
        ror_response = fetch_ror_response(ror_shortcode)
        ror_homepage_url = get_homepage_url_from_ror(ror_response)

    wikidata_homepage_url = remove_trailing_slash(wikidata_homepage_url)
    ror_homepage_url = remove_trailing_slash(ror_homepage_url)

    preferred_url = wikidata_homepage_url or ror_homepage_url
    preferred_url = prefer_https_version(preferred_url, wikidata_homepage_url, ror_homepage_url)

    return preferred_url


def get_homepage_url_from_wikidata(wikidata_response, wikidata_shortcode):
    if not wikidata_response:
        return None

    try:
        claims = wikidata_response["entities"][wikidata_shortcode]["claims"]
        if "P856" in claims:
            return claims["P856"][0]["mainsnak"]["datavalue"]["value"]
    except KeyError:
        print(f"Error getting homepage url from wikidata for {wikidata_shortcode}")
        return None


def get_homepage_url_from_ror(ror_response):
    if ror_response and "links" in ror_response and ror_response["links"]:
        return ror_response["links"][0]
    return None


def remove_trailing_slash(url):
    return url.rstrip("/") if url else None


def prefer_https_version(preferred_url, wikidata_homepage_url, ror_homepage_url):
    if preferred_url and preferred_url.startswith("http://"):
        https_url = preferred_url.replace("http://", "https://")
        if https_url in [wikidata_homepage_url, ror_homepage_url]:
            return https_url
    return preferred_url
