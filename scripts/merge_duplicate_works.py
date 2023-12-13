import pandas as pd
import requests


"""
This script takes known duplicates and decides which work to merge into.
"""


def get_paper_details(paper_ids):
    formatted_paper_ids = "|".join([f"W{paper_id}" for paper_id in paper_ids])
    url = f"https://api.openalex.org/works?filter=ids.openalex:{formatted_paper_ids}&select=id,ids,locations,authorships&mailto=team@ourresearch.org"
    data = requests.get(url).json()["results"]
    paper_details = []

    for paper in data:
        paper_id = paper["id"].replace("https://openalex.org/W", "")
        id_count = len(paper.get("ids", []))
        location_count = len(paper.get("locations", []))
        author_count = len(paper.get("authorships", []))
        has_affiliations = any(
            [
                author.get("raw_affiliation_string")
                for author in paper.get("authorships", [])
            ]
        )

        paper_details.append(
            {
                "paper_id": paper_id,
                "id_count": id_count,
                "location_count": location_count,
                "author_count": author_count,
                "has_affiliations": has_affiliations,
            }
        )
    return paper_details


def merge(paper_ids):
    papers = get_paper_details(paper_ids)

    # sort by has_affiliations, id_count, location_count, author_count
    papers.sort(
        key=lambda x: (
            x["has_affiliations"],
            x["id_count"],
            x["location_count"],
            x["author_count"],
        ),
        reverse=True,
    )
    print(papers)

    # if everything is the same, keep the lowest number id
    if (
        papers[0]["id_count"] == papers[1]["id_count"]
        and papers[0]["location_count"] == papers[1]["location_count"]
        and papers[0]["author_count"] == papers[1]["author_count"]
        and papers[0]["has_affiliations"] == papers[1]["has_affiliations"]
    ):
        papers.sort(key=lambda x: x["paper_id"])

    # first paper after sorting is the one to merge into
    merge_into_id = papers[0]["paper_id"]
    # return a tuple of the merge_into_id and the other ids
    return merge_into_id, [paper["paper_id"] for paper in papers[1:]]


if __name__ == "__main__":
    df = pd.read_parquet("sample_hal_duplicates.parquet")

    with open("ids_to_merge.csv", "a") as f:
        # header row
        f.write("old_id,merge_into_id\n")
    for index, row in df.iterrows():
        try:
            print(f"Index: {index}")
            paper_ids = row["paper_ids"]
            hal_ids = row["hal_ids"]
            print(f"Paper ids: {paper_ids}")
            print(f"Hal ids: {hal_ids}")
            print("-----------------")
            merge_into_id, old_ids = merge(paper_ids)
            for old_id in old_ids:
                with open("ids_to_merge.csv", "a") as f:
                    f.write(f"{old_id},{merge_into_id}\n")
        except Exception as e:
            print(f"Error: {e}")
            continue
