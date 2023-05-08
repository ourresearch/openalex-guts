from scripts.wikidata_ror.cached_session import cached_session
from scripts.wikidata_ror.normalize import normalize_ror


def fetch_ror_response(ror_id):
    ror_shortcode = normalize_ror(ror_id)
    ror_url = f"https://api.ror.org/organizations/{ror_shortcode}"
    session = cached_session()
    response = session.get(ror_url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching ROR response")
        return None


def fetch_wikidata_response(wikidata_shortcode):
    wikidata_url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={wikidata_shortcode}&format=json"
    session = cached_session()
    response = session.get(wikidata_url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching Wikidata response")
        return None
