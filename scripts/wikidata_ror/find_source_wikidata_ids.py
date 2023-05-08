from app import db
from models.source import Source
from scripts.wikidata_ror.find_wikidata_ids import find_wikidata_id_for_source


"""
Find missing source wikidata IDs.
Run with `python -m scripts.wikidata_ror.find_source_wikidata_ids`.
"""


def find_source_wiki_ids():
    sources = Source.query.with_entities(
        Source.display_name,
        Source.journal_id,
        Source.wikidata_id,
    ).filter(Source.wikidata_id == None).all()
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
        db.session.commit()


if __name__ == "__main__":
    find_source_wiki_ids()
