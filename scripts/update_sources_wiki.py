from app import db
from models.source import Source
from scripts.wiki_ror_utils import find_wikidata_id_for_source, get_homepage_url, get_country_code, get_image_url, get_image_thumbnail_url


def update_sources():
    sources = Source.query.with_entities(
        Source.country_code,
        Source.display_name,
        Source.image_url,
        Source.image_thumbnail_url,
        Source.journal_id,
        Source.webpage,
        Source.wikidata_id,
    ).filter(Source.wikidata_id != None).all()
    count = 0
    for source in sources:
        count += 1
        print(f"Processing {count} of {len(sources)}")
        if not source.country_code:
            country_code = get_country_code(source.wikidata_id)
            if country_code:
                print(
                    f"Updating country_code for {source.display_name} from {source.country_code} to {country_code}"
                )
                Source.query.filter(Source.journal_id == source.journal_id).update(
                    {Source.country_code: country_code}, synchronize_session=False
                )
        if not source.webpage:
            homepage_url = get_homepage_url(source.wikidata_id)
            if homepage_url:
                print(
                    f"Updating homepage_url for {source.display_name} from {source.webpage} to {homepage_url}"
                )
                Source.query.filter(Source.journal_id == source.journal_id).update(
                    {Source.webpage: homepage_url}, synchronize_session=False
                )
        if not source.image_url:
            image_url = get_image_url(source.wikidata_id)
            if image_url:
                image_thumbnail_url = get_image_thumbnail_url(image_url)
                print(
                    f"Updating image_url for {source.display_name} from {source.image_url} to {image_url}"
                )
                print(
                    f"Updating image_thumbnail_url for {source.display_name} from {source.image_thumbnail_url} to {image_thumbnail_url}"
                )
                Source.query.filter(Source.journal_id == source.journal_id).update(
                    {
                        Source.image_url: image_url,
                        Source.image_thumbnail_url: image_thumbnail_url,
                    },
                    synchronize_session=False,
                )
        # commit every time, for now
        db.session.commit()


if __name__ == "__main__":
    update_sources()
