import argparse
from collections import defaultdict
from time import sleep, time

from sqlalchemy import orm, text
from sqlalchemy.orm import lazyload, joinedload

import models
from app import db
from app import logger
from util import elapsed


def run(**kwargs):
    if single_id := kwargs.get('id'):
        if record := get_records([single_id]):
            record[0].process_record()
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
                records = get_records(record_ids)

                doi_counts = defaultdict(int)
                pmid_counts = defaultdict(int)
                title_counts = defaultdict(int)

                for record in records:
                    if record.doi:
                        doi_counts[record.doi] += 1
                    if record.pmid:
                        pmid_counts[record.pmid] += 1
                    if record.normalized_title:
                        title_counts[record.normalized_title] += 1

                unique_records = [
                    r for r in records if
                    (not r.doi or doi_counts[r.doi] == 1) and
                    (not r.pmid or pmid_counts[r.pmid] == 1) and
                    (not r.normalized_title or title_counts[r.normalized_title] == 1)
                ]

                unique_record_ids = [u.id for u in unique_records]

                possibly_colliding_records = [r for r in records if r.id not in unique_record_ids]

                start = time()
                for u in unique_records:
                    u.process_record()

                db.session.commit()
                logger.info(f'mapped {len(unique_records)} unique records in {elapsed(start, 2)} seconds')

                for p in possibly_colliding_records:
                    start = time()
                    p.process_record()
                    db.session.commit()
                    logger.info(f'did {p.id} in {elapsed(start, 2)} seconds')

                finish_object_ids(record_ids)
                objects_updated += len(records)
                logger.info(f'processed chunk of {chunk} records in {elapsed(loop_start, 2)} seconds')
            else:
                logger.info('nothing ready in the queue, waiting 5 seconds...')
                sleep(5)


def fetch_queue_chunk_ids(chunk_size):
    text_query = """
        select id from ins.recordthresher_record
        where work_id is null
        order by updated asc nulls last
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
    logger.info(f'getting records')

    record = db.session.query(models.Record).options(
        lazyload(models.Record.work_matches_by_title).raiseload('*'),
        lazyload(models.Record.work_matches_by_title).joinedload(models.Work.affiliations).raiseload('*'),
        joinedload(models.Record.work_matches_by_doi).raiseload('*'),
        joinedload(models.Record.work_matches_by_pmid).raiseload('*'),
        joinedload(models.Record.work_matches_by_arxiv_id).raiseload('*'),
        joinedload(models.Record.journals).raiseload('*'),
        joinedload(models.Record.journals).selectinload(models.Source.merged_into_source).raiseload('*'),
        joinedload(models.Record.parseland_record).raiseload('*'),
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