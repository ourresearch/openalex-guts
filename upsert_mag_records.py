from datetime import datetime
from sqlalchemy import text
from scripts.add_things_queue import enqueue_jobs

import redis
import shortuuid

from app import db, REDIS_QUEUE_URL

UPSERT_QUEUE = 'queue:mag_authors_upsert'
REDIS = redis.from_url(REDIS_QUEUE_URL)
BATCH_SIZE = 100


def make_recordthresher_id():
    return shortuuid.uuid()[:22]


def update_or_insert_record(session, work_id):
    update_query = """
        UPDATE ins.recordthresher_record
        SET authors = mag_authors.authors
        FROM ins.mag_authors AS mag_authors
        WHERE ins.recordthresher_record.work_id = mag_authors.work_id
          AND ins.recordthresher_record.record_type = 'mag_location'
          AND ins.recordthresher_record.work_id = :work_id;
    """

    result = session.execute(text(update_query), {'work_id': work_id})

    if result.rowcount == 0:
        record_id = make_recordthresher_id()
        insert_query = """
            INSERT INTO ins.recordthresher_record (id, work_id, record_type, authors)
            SELECT :id, :work_id, 'mag_location', mag_authors.authors
            FROM ins.mag_authors
            WHERE work_id = :work_id;
        """
        session.execute(text(insert_query),
                        {'id': record_id, 'work_id': work_id})

    session.commit()


def dequeue_work_ids(num):
    work_ids_bytes = REDIS.spop(UPSERT_QUEUE, num)
    return [int(work_id.decode('utf-8')) for work_id in work_ids_bytes]


if __name__ == '__main__':
    count = 0
    start = datetime.now()
    while True:
        print(f'Popping {BATCH_SIZE} work ids from {UPSERT_QUEUE}')
        work_ids = dequeue_work_ids(BATCH_SIZE)
        print(f'Popped {len(work_ids)} work ids from {UPSERT_QUEUE}')

        for work_id in work_ids:
            update_or_insert_record(db.session, work_id)
            count += 1
        last_work_id = work_ids[-1]
        mark_updated_query = '''UPDATE ins.mag_authors SET finished = true WHERE work_id IN :work_ids'''
        db.session.execute(text(mark_updated_query),
                           {'work_ids': tuple(work_ids)})
        enqueue_jobs(work_ids, methods=None, fast_queue_priority=-1)
        now = datetime.now()
        elapsed_hrs = (now - start).total_seconds() / 3600
        rate = round(count / elapsed_hrs, 2)
        print(f'Processed: {count} | Rate: {rate}/hr | Last ID: {last_work_id}')
