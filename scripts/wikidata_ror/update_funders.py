from app import db
from models.funder import Funder
from scripts.wikidata_ror.alternate_titles import get_alternate_titles
from scripts.wikidata_ror.description import get_description
from scripts.wikidata_ror.homepage import get_homepage_url
from scripts.wikidata_ror.image_url import get_image_url, get_image_thumbnail_url


"""
Update funders with data from Wikidata and ROR.
Run with `python -m scripts.wikidata_ror.update_funders`.
"""


def update_funders():
    funders = Funder.query.with_entities(
        Funder.alternate_titles,
        Funder.country_code,
        Funder.description,
        Funder.display_name,
        Funder.funder_id,
        Funder.homepage_url,
        Funder.image_thumbnail_url,
        Funder.image_url,
        Funder.ror_id,
        Funder.wikidata_id
    ).filter((Funder.ror_id != None) | (Funder.wikidata_id != None)).all()
    count = 0
    for funder in funders:
        count += 1
        print(f"Processing {count} of {len(funders)}")
        if not funder.alternate_titles:
            alternate_titles = get_alternate_titles(funder.wikidata_id, funder.ror_id)
            if alternate_titles:
                print(
                    f"Adding alternate_titles for {funder.funder_id} with {alternate_titles}"
                )
                Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                    {Funder.alternate_titles: alternate_titles},
                    synchronize_session=False,
                )
        description = get_description(funder.wikidata_id)
        if description and funder.description != description:
            print(
                f"Updating description for {funder.funder_id} from {funder.description} to {description}"
            )
            Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                {Funder.description: description}, synchronize_session=False
            )

        homepage_url = get_homepage_url(funder.wikidata_id, funder.ror_id)
        if homepage_url and funder.homepage_url != homepage_url:
            print(
                f"Updating homepage_url for {funder.funder_id} from {funder.homepage_url} to {homepage_url}"
            )
            Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                {Funder.homepage_url: homepage_url}, synchronize_session=False
            )

        if not funder.image_url or not funder.image_thumbnail_url:
            # only add image_url if it doesn't already exist
            image_url = get_image_url(funder.wikidata_id)
            image_thumbnail_url = get_image_thumbnail_url(image_url)
            if image_url:
                print(
                    f"Adding image_url for {funder.funder_id} with {image_url} and {image_thumbnail_url}"
                )
                Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                    {
                        Funder.image_url: image_url,
                        Funder.image_thumbnail_url: image_thumbnail_url,
                    },
                    synchronize_session=False,
                )
        # commit every time, for now
        db.session.commit()


if __name__ == "__main__":
    update_funders()
