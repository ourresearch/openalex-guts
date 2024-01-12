import argparse
from time import sleep
from time import time
import random

from sqlalchemy import orm
from sqlalchemy import text
from sqlalchemy.orm import selectinload

import models
from models.work_embedding import get_and_save_embeddings
from app import db, logger
from util import elapsed

"""
Run with: heroku local:run python -m scripts.queue_work_process_embeddings --chunk=100
"""


def process_embeddings(work):
    print(f"Processing {work.id}")
    get_and_save_embeddings(work)
    db.session.execute('''
                    UPDATE queue.run_once_work_process_embeddings 
                    SET finished = NOW() 
                    WHERE work_id = :work_id
                ''', {'work_id': work.id})
    db.session.commit()
    print(f"Processed {work.id}")


class QueueWorkProcessEmbeddings:
    def worker_run(self, **kwargs):
        single_id = kwargs.get("id", None)
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)

        if limit is None:
            limit = float("inf")

        if single_id:
            work = QueueWorkProcessEmbeddings.fetch_works([single_id])[0]
            db.session.execute('''
                UPDATE queue.run_once_work_process_embeddings
                SET started = NOW() 
                WHERE work_id = :work_id
            ''', {'work_id': single_id})
            db.session.commit()
            process_embeddings(work)
        else:
            num_updated = 0

            while num_updated < limit:
                start_time = time()

                work_ids = self.fetch_and_lock_queue_chunk(chunk_size)

                if not work_ids:
                    logger.info('no queued Works ready to process... waiting.')
                    sleep(60)
                    continue

                works = QueueWorkProcessEmbeddings.fetch_works(work_ids)

                for work in works:
                    logger.info(f'running process_embeddings on {work}')
                    try:
                        process_embeddings(work)
                    except Exception as e:
                        logger.error(f'error processing {work} - {e}')
                        continue

                db.session.execute('''
                    UPDATE queue.run_once_work_process_embeddings
                    SET finished = NOW() 
                    WHERE work_id = any(:work_ids)
                ''', {'work_ids': work_ids})
                db.session.commit()

                commit_start_time = time()
                db.session.commit()
                logger.info(f'commit took {elapsed(commit_start_time, 2)} seconds')

                num_updated += chunk_size
                logger.info(f'processed {len(work_ids)} Works in {elapsed(start_time, 2)} seconds')

    @staticmethod
    def fetch_and_lock_queue_chunk(chunk_size):
        logger.info("looking for works to update embeddings on")

        with db.engine.begin() as connection:
            queue_query = text(f"""
                SELECT work_id
                FROM queue.run_once_work_process_embeddings
                WHERE started IS NULL
                LIMIT :chunk
                FOR UPDATE SKIP LOCKED
            """).bindparams(chunk=chunk_size)

            id_list = [row[0] for row in connection.execute(queue_query).all()]

            # Immediately mark the fetched IDs as started within the same transaction
            connection.execute("""
                UPDATE queue.run_once_work_process_embeddings
                SET started = NOW() 
                WHERE work_id = ANY(%(id_list)s)
            """, {'id_list': id_list})

        logger.info(f'got {len(id_list)} IDs to process')

        return id_list

    @staticmethod
    def base_works_query():
        return db.session.query(models.Work).options(
            selectinload(models.Work.abstract).raiseload('*'),
            orm.Load(models.Work).raiseload('*')
        )

    @staticmethod
    def fetch_works(object_ids):
        job_time = time()

        try:
            objects = QueueWorkProcessEmbeddings.base_works_query().filter(
                models.Work.paper_id.in_(object_ids)
            ).all()
        except Exception as e:
            logger.exception(f'exception getting records for {object_ids} so trying individually')
            objects = []
            for object_id in object_ids:
                try:
                    objects += QueueWorkProcessEmbeddings.base_works_query().filter(
                        models.Work.paper_id == object_id
                    ).all()
                except Exception as e:
                    logger.exception(f'failed to load object {object_id}')

        logger.info(f'got {len(objects)} Works, took {elapsed(job_time)} seconds')

        return objects


if __name__ == "__main__":
    sleep(random.randint(0, 60))
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', nargs="?", type=str, help="id of the Work embeddings you want to update")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many Works to update")
    parser.add_argument('--chunk', "-ch", nargs="?", default=10, type=int, help="how many Works to update at once")
    parsed_args = parser.parse_args()

    my_queue = QueueWorkProcessEmbeddings()
    my_queue.worker_run(**vars(parsed_args))
