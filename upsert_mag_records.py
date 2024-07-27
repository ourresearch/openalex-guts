from datetime import datetime

from sqlalchemy import text

from app import db
import shortuuid

from scripts.helpers.enqueue_add_some_things import enqueue_works


def make_recordthresher_id():
    return shortuuid.uuid()[:22]


def update_or_insert_record(session, row):
    # Define the update query
    update_query = """
        UPDATE ins.recordthresher_record
        SET authors = mag_authors.authors
        FROM ins.mag_authors AS mag_authors
        WHERE ins.recordthresher_record.work_id = mag_authors.work_id
          AND ins.recordthresher_record.record_type = 'mag_location'
          AND ins.recordthresher_record.work_id = :work_id;
    """

    # Execute the update query
    result = session.execute(text(update_query), {'work_id': row['work_id']})

    # If no rows were updated, perform the insert
    if result.rowcount == 0 and row['authors']:
        insert_query = """
            INSERT INTO ins.recordthresher_record (id, work_id, record_type, authors)
            VALUES (:id, :work_id, 'mag_location', :authors);
        """
        update = {'id': make_recordthresher_id()}
        update.update(row)
        session.execute(text(insert_query), update)
    session.commit()


def dequeue_and_mark_processing(session, batch_size=100):
    cte_query = """
        WITH rows_to_process AS (
            SELECT *
            FROM ins.mag_authors
            WHERE (processing IS NULL OR processing = FALSE) AND authors LIKE '%affiliations": [{%'
            ORDER BY work_id
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE ins.mag_authors
        SET processing = TRUE
        FROM rows_to_process
        WHERE ins.mag_authors.work_id = rows_to_process.work_id
        RETURNING rows_to_process.*;
    """

    result = session.connection().execution_options(autocommit=True).execute(text(cte_query), {'batch_size': batch_size})
    session.commit()
    return result.mappings().all()


if __name__ == '__main__':
    last_work_id = None
    count = 0
    start = datetime.now()
    while True:
        rows = dequeue_and_mark_processing(db.session, batch_size=100)
        if not rows:
            break

        for row in rows:
            update_or_insert_record(db.session, row)
            count += 1
        last_work_id = rows[-1].work_id
        mark_updated_query = '''UPDATE ins.mag_authors SET finished = true WHERE work_id IN :work_ids'''
        work_ids = [row['work_id'] for row in rows]
        db.session.execute(text(mark_updated_query), {'work_ids': tuple(work_ids)})
        enqueue_works(work_ids, methods=None, fast_queue_priority=-1)
        now = datetime.now()
        elapsed_hrs = (now - start).total_seconds() / 3600
        rate = round(count / elapsed_hrs, 2)
        print(f'Processed: {count} | Rate: {rate}/hr | Last ID: {last_work_id}')
