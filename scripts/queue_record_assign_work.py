import argparse
from time import sleep, time

from sqlalchemy import orm, text
from sqlalchemy.orm import lazyload, joinedload

import models
from app import db
from app import logger
from util import elapsed


def run(**kwargs):
    if single_id := kwargs.get('id'):
        if record := get_record(single_id):
            record.process_record()
            finish_object_ids([single_id])
        else:
            logger.warn(f'found no recordthresher record with id {single_id}')
    else:
        objects_updated = 0
        limit = kwargs.get('limit')
        chunk = kwargs.get('chunk')

        while limit is None or objects_updated < limit:
            if record_ids := fetch_queue_chunk_ids(chunk):
                loop_start = time()
                for record_id in record_ids:
                    record_start_time = time()
                    # No point bulk loading objects. When a new work is created it commits itself,
                    # and later objects in the batch must be reloaded.

                    logger.info(f"*** #{objects_updated}: starting {record_id}.process_record()")

                    start_time = time()
                    record = get_record(record_id)
                    logger.info(f"fetched recordthresher record {record_id} in {elapsed(start_time, 4)} seconds")

                    start_time = time()
                    record.process_record()
                    logger.info(f"processed recordthresher record {record_id} in {elapsed(start_time, 4)} seconds")

                    objects_updated += 1

                    logger.info(f"*** finished {record_id}.process_record() in {elapsed(record_start_time, 4)} seconds")

                finish_object_ids(record_ids)
                logger.info(f'processed chunk of {chunk} records in {elapsed(loop_start, 2)} seconds')
            else:
                logger.info('nothing ready in the queue, waiting 5 seconds...')
                sleep(5)


def fetch_queue_chunk_ids(chunk_size):
    text_query = """
        select id from ins.recordthresher_record
        where work_id is null
        order by updated desc nulls last
        limit :chunk;
    """

    logger.info(f'getting {chunk_size} record IDs from the queue')
    start_time = time()

    ids = [
        row[0] for row in
        db.engine.execute(text(text_query).bindparams(chunk=chunk_size).execution_options(autocommit=True)).all()
    ]

    logger.info(f'got {len(ids)} ids from the queue in {elapsed(start_time, 4)}s')
    logger.info(f'got these ids: {ids}')

    return ids


def finish_object_ids(object_ids):
    start_time = time()
    db.session.commit()
    logger.info(f'committing records and works took {elapsed(start_time, 2)} seconds')

    start_time = time()
    db.session.execute(
        text(
            '''
            insert into queue.run_once_work_add_everything
            (work_id, work_updated)
            (
                select work_id, updated
                from ins.recordthresher_record
                where id = any(:record_ids)
                and work_id > 0
            )
            on conflict do nothing
            '''
        ).bindparams(record_ids=object_ids).execution_options(autocommit=True)
    )
    logger.info(f'enqueueing mapped works took {elapsed(start_time, 2)} seconds')


def get_record(record_id):
    logger.info(f'getting record {record_id}')

    record = db.session.query(models.Record).options(
        joinedload(models.Record.work_matches_by_title).raiseload('*'),
        lazyload(models.Record.work_matches_by_title).selectinload(models.Work.affiliations).raiseload('*'),
        joinedload(models.Record.work_matches_by_doi).raiseload('*'),
        joinedload(models.Record.work_matches_by_pmid).raiseload('*'),
        joinedload(models.Record.journals).raiseload('*'),
        orm.Load(models.Record).raiseload('*')
    ).filter(models.Record.id == record_id).scalar()

    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assign or mint Work IDs for recordthresher records.")
    parser.add_argument('--id', nargs="?", type=str, help="id of the one thing you want to update (case sensitive)")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many objects to work on")
    parser.add_argument(
        '--chunk', "-ch", nargs="?", default=100, type=int, help="how many objects to take off the queue at once"
    )

    parsed_args = parser.parse_args()
    run(**vars(parsed_args))