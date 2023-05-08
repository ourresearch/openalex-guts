from scripts.wikidata_ror.normalize import normalize_wikidata_id
from scripts.wikidata_ror.response import fetch_wikidata_response


def get_image_url(wikidata_id):
    wikidata_image_url = None
    if wikidata_id:
        wikidata_shortcode = normalize_wikidata_id(wikidata_id)
        wikidata_response = fetch_wikidata_response(wikidata_shortcode)
        if wikidata_response:
            try:
                # try to get the logo first
                claims = wikidata_response["entities"][wikidata_shortcode]["claims"]
                if "P154" in claims:
                        wikidata_image_file = claims["P154"][0]["mainsnak"]["datavalue"][
                            "value"
                        ]
                        wikidata_image_url = f"https://commons.wikimedia.org/w/index.php?title=Special:Redirect/file/{wikidata_image_file}"

                # if no logo, try to get an image
                elif "P18" in claims:
                        wikidata_image_file = claims["P18"][0]["mainsnak"]["datavalue"]["value"]
                        wikidata_image_url = f"https://commons.wikimedia.org/w/index.php?title=Special:Redirect/file/{wikidata_image_file}"
            except KeyError:
                print(f"Error getting image url for {wikidata_shortcode}")
    return wikidata_image_url


def get_image_thumbnail_url(image_url):
    if image_url:
        return f"{image_url}&width=300"