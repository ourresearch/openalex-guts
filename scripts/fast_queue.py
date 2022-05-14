import argparse
from time import sleep, time

from sqlalchemy import orm, text
from sqlalchemy.orm import selectinload

import models
from app import db
from app import logger
from scripts.queue import JsonWorks, JsonAuthors, JsonConcepts, JsonInstitutions, JsonVenues
from util import elapsed


def run(**kwargs):
    entity_type = kwargs.get("entity")
    method_name = kwargs.get("method")
    if entity_type == "work" and method_name == "add_everything":
        queue_table = "queue.work_add_everything"

    if single_id := kwargs.get('id'):
        if objects := get_objects(entity_type, [single_id]):
            logger.info(f'found object {objects[0]}')
            store_objects(objects)
            db.session.commit()
        else:
            logger.warn(f'found no object with id {single_id}')
    else:
        objects_updated = 0
        limit = kwargs.get('limit')
        chunk = kwargs.get('chunk')
        total_count = 0

        while limit is None or objects_updated < limit:
            loop_start = time()
            if object_ids := fetch_queue_chunk_ids(queue_table, chunk):
                objects = get_objects(entity_type, object_ids)

                for obj in objects:
                    method_start_time = time()
                    total_count += 1

                    print(f"*** #{total_count} starting {obj}.{method_name}() method")

                    method_to_run = getattr(obj, method_name)
                    method_to_run()

                    print(f">>> finished {obj}.{method_name}(). took {elapsed(method_start_time, 4)} seconds")

                finish_object_ids(queue_table, object_ids)

                objects_updated += len(objects)

                logger.info('committing')
                start_time = time()
                # fail loudly for now
                db.session.commit()

                # commit_success = safe_commit(db)
                #
                # if not commit_success:
                #     logger.info("COMMIT fail")

                logger.info(f'commit took {elapsed(start_time, 4)}s')
                logger.info(f'processed chunk of {chunk} objects in {elapsed(loop_start, 2)} seconds')
            else:
                logger.info('nothing ready in the queue, waiting 5 seconds...')
                sleep(5)


def store_objects(objects):
    json_row_dicts = []
    logger.info(f'calculating json rows')
    start_time = time()
    for obj in objects:
        obj.store()
        for insert_dict in obj.insert_dicts:
            if insert_dict_value := insert_dict.get('JsonAuthors'):
                json_row_dicts.append(insert_dict_value)

    logger.info(f'made {len(json_row_dicts)} json rows in {elapsed(start_time, 4)}s')

    logger.info('merging json rows')

    start_time = time()
    for json_row_dict in json_row_dicts:
        json_author = JsonAuthors(**json_row_dict)
        db.session.merge(json_author)
    logger.info(f'merged json rows in {elapsed(start_time, 4)}s')


def fetch_queue_chunk_ids(queue_table, chunk_size):
    text_query = f"""
          with chunk as (
              select id
              from {queue_table}
              where started is null
              order by
                  finished asc nulls first,
                  rand
              limit :chunk
              for update skip locked
          )
          update {queue_table}
          set started = now()
          from chunk
          where {queue_table}.id = chunk.id
          returning chunk.id;
    """

    logger.info(f'getting {chunk_size} ids from the queue')
    start_time = time()

    ids = [
        row[0] for row in
        db.engine.execute(text(text_query).bindparams(chunk=chunk_size).execution_options(autocommit=True)).all()
    ]

    logger.info(f'got {len(ids)} ids from the queue in {elapsed(start_time, 4)}s')

    return ids


def finish_object_ids(queue_table, object_ids):
    logger.info(f'finishing queue chunk')
    start_time = time()

    query_text = f'''
        update {queue_table}
        set finished = now(), started=null
        where id = any(:ids)
    '''

    db.session.execute(text(query_text).bindparams(ids=object_ids))
    logger.info(f'finished queue chunk in {elapsed(start_time, 4)}s')


def get_objects(entity_type, object_ids):
    logger.info(f'getting {len(object_ids)} objects')

    start_time = time()
    if entity_type == "work":
        objects = db.session.query(models.Work).options(
             selectinload(models.Work.records).selectinload(models.Record.journals).raiseload('*'),
             selectinload(models.Work.records).raiseload('*'),
             selectinload(models.Work.locations).raiseload('*'),
             selectinload(models.Work.journal).raiseload('*'),
             selectinload(models.Work.references).raiseload('*'),
             selectinload(models.Work.mesh),
             selectinload(models.Work.counts_by_year).raiseload('*'),
             selectinload(models.Work.abstract),
             selectinload(models.Work.extra_ids).raiseload('*'),
             selectinload(models.Work.related_works).raiseload('*'),
             selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids).raiseload('*'),
             selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).raiseload('*'),
             selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
             selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
             selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept).raiseload('*'),
             selectinload(models.Work.concepts_full).raiseload('*'),
             orm.Load(models.Work).raiseload('*')
        ).filter(models.Work.paper_id.in_(object_ids)).all()
    elif entity_type == "author":
        objects = db.session.query(models.Author).options(
            selectinload(models.Author.counts_by_year_papers),
            selectinload(models.Author.counts_by_year_citations),
            selectinload(models.Author.alternative_names),
            selectinload(models.Author.author_concepts),
            selectinload(models.Author.orcids).selectinload(models.AuthorOrcid.orcid_data),
            selectinload(models.Author.last_known_institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Author.last_known_institution).raiseload('*'),
            orm.Load(models.Author).raiseload('*')
        ).filter(models.Author.id.in_(object_ids)).all()
    logger.info(f'got {len(objects)} objects in {elapsed(start_time, 4)}s')
    return objects



# python -m scripts.fast_queue --entity=work --method=add_everything --limit=3
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run fast queue.")
    parser.add_argument('--entity', type=str, help="the entity type to run")
    parser.add_argument('--method', type=str, help="the method to run")
    parser.add_argument('--id', nargs="?", type=str, help="id of the one thing you want to update (case sensitive)")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many objects to work on")
    parser.add_argument(
        '--chunk', "-ch", nargs="?", default=100, type=int, help="how many objects to take off the queue at once"
    )

    parsed_args = parser.parse_args()
    run(**vars(parsed_args))
