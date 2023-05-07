from app import db
from models.source import Source
from scripts.wiki_ror_utils import find_wikidata_id_for_source


def update_sources():
    sources = Source.query.with_entities(
        Source.country_code,
        Source.display_name,
        Source.journal_id,
        Source.webpage,
        Source.wikidata_id,
    ).all()
    count = 0
    for source in sources:
        count += 1
        print(f"Processing {count} of {len(sources)}")
        if not source.wikidata_id:
            wikidata_id = find_wikidata_id_for_source(source.display_name)
            if wikidata_id:
                print(
                    f"Updating wikidata_id for {source.display_name} from {source.wikidata_id} to {wikidata_id}"
                )
                Source.query.filter(Source.journal_id == source.journal_id).update(
                    {Source.wikidata_id: wikidata_id}, synchronize_session=False
                )
        # commit every time, for now
        db.session.commit()


if __name__ == "__main__":
    update_sources()
