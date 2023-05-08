import requests


def find_wikidata_id(display_name, words_to_ignore=None, words_to_include=None):
    # search the wikidata API for the display_name
    r = requests.get(
        f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={display_name}&language=en&format=json"
    )
    if r.status_code == 200:
        response = r.json()
        for result in response["search"]:
            if (
                    len(response["search"]) == 1
                    or result["label"].lower() == display_name.lower()
            ):
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


def find_wikidata_id_for_source(display_name):
    # search the wikidata API for the display_name
    r = requests.get(
        f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={display_name}&language=en&format=json"
    )
    if r.status_code == 200:
        try:
            response = r.json()
        except Exception as e:
            print(f"Error parsing json for {display_name}")
            return None
        for result in response["search"]:
            if (
                len(response["search"]) == 1
                or result["label"].lower() == display_name.lower()
            ):
                wikidata_shortcode = result["id"]
                r2 = requests.get(
                    f"https://www.wikidata.org/w/api.php?action=wbgetclaims&entity={wikidata_shortcode}&format=json"
                )
                if r2.status_code == 200:
                    response2 = r2.json()
                    if (
                        is_instance_of_scientific_journal(response2)
                        or is_instance_of_online_database(response2)
                        or has_issn_or_issnl(response2)
                        or has_source_words_in_description(response2)
                    ):
                        wikidata_id = (
                            f"https://www.wikidata.org/entity/{wikidata_shortcode}"
                        )
                        return wikidata_id
                    else:
                        wikidata_id = (
                            f"https://www.wikidata.org/entity/{wikidata_shortcode}"
                        )
                        print(f"Not a scientific journal or repository {wikidata_id}")
                        return None


def is_instance_of_online_database(response):
    claims = response.get("claims", {})
    online_databases = ["Q212805", "Q1789476", "Q1916557", "Q7096323"]
    for db in online_databases:
        P31 = claims.get("P31", [])
        for P31_item in P31:
            value_id = P31_item.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")
            if value_id == db:
                print("Is instance of online database")
                return True

    return False


def is_instance_of_scientific_journal(response):
    claims = response.get("claims", {})
    P31 = claims.get("P31", [])

    for P31_item in P31:
        value_id = P31_item.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id")

        if value_id == "Q5633421":
            print("Is instance of scientific journal")
            return True

    return False


def has_issn_or_issnl(response):
    claims = response.get("claims", {})
    P236 = claims.get("P236", [])
    P7363 = claims.get("P7363", [])

    if len(P236) > 0 or len(P7363) > 0:
        print("Has ISSN or ISSN-L")
        return True

    return False


def has_source_words_in_description(response):
    description = response.get("description", "").lower()
    if "journal" in description or "repository" in description:
        print("Has journal or repository in description")
        return True
