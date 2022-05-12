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
    if single_id := kwargs.get('id'):
        if objects := get_author_objects([single_id]):
            logger.info(f'found object {objects[0]}')
            store_json_objects(objects)
            db.session.commit()
        else:
            logger.warn(f'found no object with id {single_id}')
    else:
        objects_updated = 0
        limit = kwargs.get('limit')
        chunk = kwargs.get('chunk')

        while limit is None or objects_updated < limit:
            loop_start = time()
            if object_ids := fetch_queue_chunk_ids(chunk):
                objects = get_author_objects(object_ids)
                store_json_objects(objects)
                finish_object_ids(object_ids)

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


def store_json_objects(objects):
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


def fetch_queue_chunk_ids(chunk_size):
    text_query = """
          with chunk as (
              select id
              from queue.author_store
              where started is null
              order by
                  finished asc nulls first,
                  rand
              limit :chunk
              for update skip locked
          )
          update queue.author_store
          set started = now()
          from chunk
          where queue.author_store.id = chunk.id
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


def finish_object_ids(object_ids):
    logger.info(f'finishing queue chunk')
    start_time = time()

    query_text = '''
        update queue.author_store
        set finished = now(), started=null
        where id = any(:ids)
    '''

    db.session.execute(text(query_text).bindparams(ids=object_ids))
    logger.info(f'finished queue chunk in {elapsed(start_time, 4)}s')


def get_author_objects(object_ids):
    logger.info(f'getting {len(object_ids)} objects')

    start_time = time()
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run fast queue.")
    parser.add_argument('--id', nargs="?", type=str, help="id of the one thing you want to update (case sensitive)")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many objects to work on")
    parser.add_argument(
        '--chunk', "-ch", nargs="?", default=100, type=int, help="how many objects to take off the queue at once"
    )
    # table
    # method

    parsed_args = parser.parse_args()
    run(**vars(parsed_args))
