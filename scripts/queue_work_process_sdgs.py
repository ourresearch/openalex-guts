import argparse
import json
from time import sleep
from time import time
import random

import requests
from sqlalchemy import orm
from sqlalchemy import text
from sqlalchemy.orm import selectinload

import models
from app import db, logger, SDG_CLASSIFIER_URL
from util import elapsed

"""
Run with: heroku local:run python -m scripts.queue_work_process_sdgs --chunk=100
"""


def process_sdg(work):
    print(f"Processing {work.id}")
    if work.abstract and work.abstract.abstract and work.work_title:
        text_to_process = work.work_title + " " + work.abstract.abstract
    elif not work.abstract and work.work_title:
        text_to_process = work.work_title
    elif work.abstract and work.abstract.abstract and work.work_title is None:
        text_to_process = work.abstract.abstract
    else:
        print(f"Error processing {work.id} - no text to process")
        return None
    url = SDG_CLASSIFIER_URL

    data = {"text": text_to_process}
    r = requests.post(url, json=data)
    if r.status_code == 200:
        result = r.json()
        # replace http in id field to https
        modified = []
        for item in result:
            item["sdg"]["id"] = item["sdg"]["id"].replace("http://", "https://")
            modified.append(item)
        result_sorted = sorted(modified, key=lambda x: x["prediction"], reverse=True)
        db.session.execute(
            text(
                """
            INSERT INTO mid.work_sdg (paper_id, predictions)
            VALUES (:paper_id, :predictions)
            ON CONFLICT (paper_id) DO UPDATE SET predictions = :predictions
            """
            ),
            {"paper_id": work.id, "predictions": json.dumps(result_sorted)},
        )
        db.session.execute('''
                        UPDATE queue.run_once_work_process_sdgs 
                        SET finished = NOW() 
                        WHERE work_id = :work_id
                    ''', {'work_id': work.id})
        db.session.commit()
        print(f"Processed {work.id}")
    else:
        print(f"Error processing {work.id} - other than 200 response from classifier")


class QueueWorkProcessSdgs:
    def worker_run(self, **kwargs):
        single_id = kwargs.get("id", None)
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)

        if limit is None:
            limit = float("inf")

        if single_id:
            work = QueueWorkProcessSdgs.fetch_works([single_id])[0]
            db.session.execute('''
                UPDATE queue.run_once_work_process_sdgs 
                SET started = NOW() 
                WHERE work_id = :work_id
            ''', {'work_id': single_id})
            db.session.commit()
            process_sdg(work)
        else:
            num_updated = 0

            while num_updated < limit:
                start_time = time()

                work_ids = self.fetch_and_lock_queue_chunk(chunk_size)

                if not work_ids:
                    logger.info('no queued Works ready to process... waiting.')
                    sleep(60)
                    continue

                works = QueueWorkProcessSdgs.fetch_works(work_ids)

                for work in works:
                    logger.info(f'running process_sdgs on {work}')
                    try:
                        process_sdg(work)
                    except Exception as e:
                        logger.error(f'error processing {work} - {e}')
                        continue

                db.session.execute('''
                    UPDATE queue.run_once_work_process_sdgs 
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
        logger.info("looking for works to update sdgs on")

        with db.engine.begin() as connection:
            queue_query = text(f"""
                SELECT work_id
                FROM queue.run_once_work_process_sdgs
                WHERE started IS NULL
                LIMIT :chunk
                FOR UPDATE SKIP LOCKED
            """).bindparams(chunk=chunk_size)

            id_list = [row[0] for row in connection.execute(queue_query).all()]

            # Immediately mark the fetched IDs as started within the same transaction
            connection.execute("""
                UPDATE queue.run_once_work_process_sdgs 
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
            objects = QueueWorkProcessSdgs.base_works_query().filter(
                models.Work.paper_id.in_(object_ids)
            ).all()
        except Exception as e:
            logger.exception(f'exception getting records for {object_ids} so trying individually')
            objects = []
            for object_id in object_ids:
                try:
                    objects += QueueWorkProcessSdgs.base_works_query().filter(
                        models.Work.paper_id == object_id
                    ).all()
                except Exception as e:
                    logger.exception(f'failed to load object {object_id}')

        logger.info(f'got {len(objects)} Works, took {elapsed(job_time)} seconds')

        return objects


if __name__ == "__main__":
    sleep(random.randint(0, 60))
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', nargs="?", type=str, help="id of the Work sdgs you want to update")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many Works to update")
    parser.add_argument('--chunk', "-ch", nargs="?", default=10, type=int, help="how many Works to update at once")
    parsed_args = parser.parse_args()

    my_queue = QueueWorkProcessSdgs()
    my_queue.worker_run(**vars(parsed_args))
