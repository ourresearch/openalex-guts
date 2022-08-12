import argparse
from time import sleep, time

from sqlalchemy import orm, text
from sqlalchemy.orm import joinedload, lazyload

import models
from app import db
from app import logger
from util import elapsed


def run(**kwargs):
    if single_id := kwargs.get('id'):
        if record := get_records([single_id]):
            record[0].get_or_mint_work()
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

                start_time = time()
                records = get_records(record_ids)
                logger.info(f'loaded {len(records)} records in {elapsed(start_time, 4)}s')

                start_time = time()
                for record in records:
                    record_start_time = time()
                    logger.info(f"*** #{objects_updated}: starting {record.id}.get_work()")
                    record.get_work()
                    logger.info(f"tried to match record {record.id} in {elapsed(record_start_time, 4)} seconds")
                    objects_updated += 1

                mapped_records = [r for r in records if r.work_id is not None]
                logger.info(f'completed first pass matching {len(mapped_records)} records in {elapsed(start_time, 4)}s')

                unmapped_records = [r for r in records if r.work_id is None]
                start_time = time()
                for um in unmapped_records:
                    record_start_time = time()
                    um.get_or_mint_work()
                    logger.info(f"got or minted a work for {um.id} in {elapsed(record_start_time, 4)} seconds")

                num_unmapped = len(unmapped_records)
                num_new_records = len(set([r.work_id for r in unmapped_records]))
                logger.info(
                    f'made {num_new_records} new works for {num_unmapped} unmapped records in {elapsed(start_time, 4)}s'
                )

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


def get_records(record_ids):
    logger.info(f'getting {len(record_ids)} records')

    record = db.session.query(models.Record).options(
        joinedload(models.Record.work_matches_by_title).raiseload('*'),
        lazyload(models.Record.work_matches_by_title).selectinload(models.Work.affiliations).raiseload('*'),
        joinedload(models.Record.work_matches_by_doi).raiseload('*'),
        joinedload(models.Record.work_matches_by_pmid).raiseload('*'),
        joinedload(models.Record.journals).raiseload('*'),
        orm.Load(models.Record).raiseload('*')
    ).filter(models.Record.id.in_(record_ids)).all()

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