from scripts.wikidata_ror.normalize import normalize_wikidata_id, normalize_ror
from scripts.wikidata_ror.response import fetch_wikidata_response, fetch_ror_response


def get_country_code(wikidata_id, ror_id=None):
    wikidata_country_code = get_wikidata_country_code(wikidata_id) if wikidata_id else None
    ror_country_code = get_ror_country_code(ror_id) if ror_id else None

    return wikidata_country_code or ror_country_code


def get_wikidata_country_code(wikidata_id):
    wikidata_shortcode = normalize_wikidata_id(wikidata_id)
    wikidata_response = fetch_wikidata_response(wikidata_shortcode)

    if not wikidata_response:
        return None

    try:
        claims = wikidata_response["entities"][wikidata_shortcode]["claims"]
        if "P17" in claims:
            wikidata_country_shortcode = claims["P17"][0]["mainsnak"]["datavalue"]["value"]["id"]
            wikidata_country_response = fetch_wikidata_response(wikidata_country_shortcode)

            if wikidata_country_response:
                return (
                    wikidata_country_response["entities"][wikidata_country_shortcode]
                    .get("claims", {})
                    .get("P297", [{}])[0]
                    .get("mainsnak", {})
                    .get("datavalue", {})
                    .get("value", None)
                )

    except KeyError:
        print(f"Error getting country code for {wikidata_shortcode}")
        return None


def get_ror_country_code(ror_id):
    ror_shortcode = normalize_ror(ror_id)
    ror_response = fetch_ror_response(ror_shortcode)

    if ror_response:
        return ror_response.get("country", {}).get("country_code", None)
    return None

