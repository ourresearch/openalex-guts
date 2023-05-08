from scripts.wikidata_ror.normalize import normalize_wikidata_id
from scripts.wikidata_ror.response import fetch_wikidata_response


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
