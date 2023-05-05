from app import db
from models.funder import Funder
from scripts.wiki_ror_utils import (
    get_homepage_url,
    get_description,
    get_image_url,
    get_image_thumbnail_url,
    find_wikidata_id,
)


def update_funders():
    funders = Funder.query.with_entities(
        Funder.country_code,
        Funder.description,
        Funder.display_name,
        Funder.funder_id,
        Funder.homepage_url,
        Funder.image_thumbnail_url,
        Funder.image_url,
        Funder.ror_id,
        Funder.uri,
        Funder.wikidata_id,
    ).all()
    for funder in funders:
        if not funder.wikidata_id:
            # try to find new wikidata_id
            words_to_ignore = "scientific article"
            wikidata_id = find_wikidata_id(funder.display_name, words_to_ignore=words_to_ignore)
            if wikidata_id:
                print(
                    f"Updating wikidata_id for {funder.display_name} from {funder.wikidata_id} to {wikidata_id}"
                )
                Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                    {Funder.wikidata_id: wikidata_id}, synchronize_session=False
                )

        homepage_url = get_homepage_url(funder.wikidata_id, funder.ror_id)
        if homepage_url and funder.homepage_url != homepage_url:
            print(
                f"Updating homepage_url for {funder.funder_id} from {funder.homepage_url} to {homepage_url}"
            )
            Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                {Funder.homepage_url: homepage_url}, synchronize_session=False
            )

        description = get_description(funder.wikidata_id)
        if description and funder.description != description:
            print(
                f"Updating description for {funder.funder_id} from {funder.description} to {description}"
            )
            Funder.query.filter(Funder.funder_id == funder.funder_id).update(
                {Funder.description: description}, synchronize_session=False
            )

        if not funder.image_url:
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
