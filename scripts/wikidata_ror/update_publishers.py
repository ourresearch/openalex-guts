from app import db
from models.publisher import Publisher
from scripts.wikidata_ror.country_code import get_country_code
from scripts.wikidata_ror.homepage import get_homepage_url
from scripts.wikidata_ror.image_url import get_image_url, get_image_thumbnail_url


"""
Update publishers with data from Wikidata and ROR.
Run with `python -m scripts.wikidata_ror.update_publishers`.
"""


def update_publishers():
    publishers = Publisher.query.with_entities(
        Publisher.country_codes,
        Publisher.display_name,
        Publisher.publisher_id,
        Publisher.homepage_url,
        Publisher.image_thumbnail_url,
        Publisher.image_url,
        Publisher.ror_id,
        Publisher.wikidata_id,
    ).filter((Publisher.ror_id != None) | (Publisher.wikidata_id != None)).all()
    for publisher in publishers:
        if not publisher.country_codes:
            # only add country_codes if it doesn't already exist
            country_code = get_country_code(publisher.wikidata_id, publisher.ror_id)
            if country_code:
                print(
                    f"Adding country_code for {publisher.wikidata_id} with {country_code}"
                )
                country_codes = [country_code]
                Publisher.query.filter(Publisher.publisher_id == publisher.publisher_id).update(
                    {Publisher.country_codes: country_codes}, synchronize_session=False
                )

        homepage_url = get_homepage_url(publisher.wikidata_id, publisher.ror_id)
        if homepage_url and publisher.homepage_url != homepage_url:
            print(
                f"Updating homepage_url for {publisher.publisher_id} from {publisher.homepage_url} to {homepage_url}"
            )
            Publisher.query.filter(Publisher.publisher_id == publisher.publisher_id).update(
                {Publisher.homepage_url: homepage_url}, synchronize_session=False
            )

        if not publisher.image_url or publisher.image_thumbnail_url:
            # only add image_url if it doesn't already exist
            image_url = get_image_url(publisher.wikidata_id)
            image_thumbnail_url = get_image_thumbnail_url(image_url)
            if image_url:
                print(
                    f"Adding image_url for {publisher.publisher_id} with {image_url} and {image_thumbnail_url}"
                )
                Publisher.query.filter(Publisher.publisher_id == publisher.publisher_id).update(
                    {
                        Publisher.image_url: image_url,
                        Publisher.image_thumbnail_url: image_thumbnail_url,
                    },
                    synchronize_session=False,
                )
        # commit every time, for now
        db.session.commit()


if __name__ == "__main__":
    update_publishers()
