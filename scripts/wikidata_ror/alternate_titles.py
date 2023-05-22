from scripts.wikidata_ror.normalize import normalize_wikidata_id, normalize_ror
from scripts.wikidata_ror.response import fetch_wikidata_response, fetch_ror_response


def get_alternate_titles(wikidata_id, ror_id=None):
    wikidata_alternate_titles = get_wikidata_alternate_titles(wikidata_id)
    ror_alternate_titles = get_ror_alternate_titles(ror_id)

    # combine the lists using a dictionary to remove case-insensitive duplicates
    combined_titles = {}
    for title in wikidata_alternate_titles + ror_alternate_titles:
        lower_title = title.lower()
        if lower_title not in combined_titles:
            combined_titles[lower_title] = title

    return list(combined_titles.values())


def get_wikidata_alternate_titles(wikidata_id):
    wikidata_shortcode = normalize_wikidata_id(wikidata_id)
    wikidata_response = fetch_wikidata_response(wikidata_shortcode)

    if not wikidata_response:
        return []

    try:
        wikidata_alternate_titles = []
        aliases = wikidata_response["entities"][wikidata_shortcode]["aliases"]
        english_aliases = aliases.get("en", [])
        for alias in english_aliases:
            wikidata_alternate_titles.append(alias["value"])
        print(f"Found these alternate Wikidata titles for {wikidata_shortcode}: {wikidata_alternate_titles}")
        return wikidata_alternate_titles
    except KeyError:
        print(f"Error getting alternate titles for {wikidata_shortcode}")
        return []


def get_ror_alternate_titles(ror_id):
    ror_shortcode = normalize_ror(ror_id)
    ror_response = fetch_ror_response(ror_shortcode)

    if ror_response:
        ror_alternate_titles = ror_response.get("aliases", [])
        print(f"Found these alternate ROR titles for {ror_shortcode}: {ror_alternate_titles}")
        return ror_alternate_titles
    return []
