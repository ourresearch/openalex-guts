from app import db
from sqlalchemy import text
import threading
from queue import Queue
import time

BATCH_SIZE = 1000
NUM_THREADS = 4


def process_batch(queue):
    while True:
        batch_num = queue.get()
        if batch_num is None:
            break

        try:
            # Mark batch as started
            result = db.session.execute(text("""
                UPDATE earliest_dates
                SET started = TRUE
                WHERE doi IN (
                    SELECT doi FROM earliest_dates
                    WHERE started = FALSE
                    ORDER BY doi
                    LIMIT :batch_size
                )
                RETURNING doi
            """), {"batch_size": BATCH_SIZE})
            batch_dois = [row[0].lower() for row in result]
            db.session.commit()

            if not batch_dois:
                queue.task_done()
                continue

            result = db.session.execute(text("""
                WITH updated_rows AS (
                    UPDATE ins.recordthresher_record rr
                    SET published_date = ed.earliest_date
                    FROM earliest_dates ed
                    WHERE ed.doi = rr.doi
                      AND ed.doi = ANY(:batch_dois)
                      AND (rr.published_date IS NULL OR ed.earliest_date < rr.published_date)
                    RETURNING 1
                )
                SELECT COUNT(*) AS rows_updated FROM updated_rows
            """), {"batch_dois": batch_dois})
            rows_updated = result.scalar()

            # Mark batch as completed
            db.session.execute(text("""
                UPDATE earliest_dates
                SET completed = TRUE
                WHERE doi = ANY(:batch_dois)
            """), {"batch_dois": batch_dois})
            db.session.commit()

            print(
                f"Processed batch {batch_num + 1}: {len(batch_dois)} DOIs. Updated {rows_updated} rows.")
        except Exception as e:
            db.session.rollback()
            print(f"Error processing batch {batch_num + 1}: {e}")
        finally:
            queue.task_done()


def main():

    result = db.session.execute(
        text("SELECT COUNT(*) AS total FROM earliest_dates WHERE started = FALSE"))
    total_records = result.scalar()

    num_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
    queue = Queue()

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=process_batch, args=(queue,))
        t.daemon = True
        t.start()
        threads.append(t)

    for i in range(num_batches):
        queue.put(i)

    queue.join()

    for _ in range(NUM_THREADS):
        queue.put(None)

    for t in threads:
        t.join()

    print("All batches processed.")


if __name__ == "__main__":
    main()