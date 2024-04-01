import argparse
import json
from time import sleep
from time import time

from sqlalchemy import orm
from sqlalchemy import text
from sqlalchemy.orm import selectinload

import models
from app import db
from app import logger
from util import elapsed


def update_authors(work):

    if work.affiliation_records_sorted:
        record_author_dict_list = json.loads(work.affiliation_records_sorted[0].authors)
        num_authors = set([a.author_sequence_number for a in work.affiliations])

        if len(record_author_dict_list) == len(num_authors):
            logger.info('running update_institutions')
            work.update_institutions(affiliation_retry_attempts=1)
        else:
            logger.info('running add_affiliations')
            work.add_references()
            work.add_affiliations(affiliation_retry_attempts=1)


class QueueWorkUpdateAuthors:
    def worker_run(self, **kwargs):
        single_id = kwargs.get("id", None)
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)

        if limit is None:
            limit = float("inf")

        if single_id:
            work = QueueWorkUpdateAuthors.fetch_works([single_id])[0]
            update_authors(work)
            db.session.execute(
                text('''
                    insert into queue.work_store (id) values (:work_id)
                    on conflict (id)
                    do update set finished = null
                ''').bindparams(work_id=single_id)
            )
            db.session.commit()
        else:
            num_updated = 0

            while num_updated < limit:
                start_time = time()

                work_ids = self.fetch_queue_chunk(chunk_size)

                if not work_ids:
                    logger.info('no queued Works ready to add authors. waiting...')
                    sleep(60)
                    continue

                works = QueueWorkUpdateAuthors.fetch_works(work_ids)

                for work in works:
                    logger.info(f'running update_authors on {work}')
                    update_authors(work)

                db.session.execute(
                    text(f'''
                        delete from queue.run_once_work_update_authors q
                        where q.work_id = any(:work_ids)
                    ''').bindparams(work_ids=work_ids)
                )

                db.session.execute(
                    text('''
                        insert into queue.work_store (id) (
                            select paper_id from mid.work
                            where paper_id = any(:work_ids)
                        )
                        on conflict (id)
                        do update set finished = null
                    ''').bindparams(work_ids=work_ids)
                )

                commit_start_time = time()
                db.session.commit()
                logger.info(f'commit took {elapsed(commit_start_time, 2)} seconds')

                num_updated += chunk_size
                logger.info(f'processed {len(work_ids)} Works in {elapsed(start_time, 2)} seconds')

    @staticmethod
    def fetch_queue_chunk(chunk_size):
        logger.info("looking for works to update authors on")

        queue_query = text(f"""
            with queue_chunk as (
                select work_id
                from queue.run_once_work_update_authors
                where started is null
                limit :chunk
                for update skip locked
            )
            update queue.run_once_work_update_authors q
            set started = now()
            from queue_chunk
            where q.work_id = queue_chunk.work_id
            returning q.work_id;
        """).bindparams(chunk=chunk_size)

        job_time = time()
        id_list = [row[0] for row in db.engine.execute(queue_query.execution_options(autocommit=True)).all()]
        logger.info(f'got {len(id_list)} IDs, took {elapsed(job_time)} seconds')

        return id_list

    @staticmethod
    def base_works_query():
        return db.session.query(models.Work).options(
            selectinload(models.Work.records).raiseload('*'),
            selectinload(models.Work.references).raiseload('*'),
            selectinload(models.Work.references_unmatched).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
            orm.Load(models.Work).raiseload('*')
        )

    @staticmethod
    def fetch_works(object_ids):
        job_time = time()

        try:
            objects = QueueWorkUpdateAuthors.base_works_query().filter(
                models.Work.paper_id.in_(object_ids)
            ).all()
        except Exception as e:
            logger.exception(f'exception getting records for {object_ids} so trying individually')
            objects = []
            for object_id in object_ids:
                try:
                    objects += QueueWorkUpdateAuthors.base_works_query().filter(
                        models.Work.paper_id == object_id
                    ).all()
                except Exception as e:
                    logger.exception(f'failed to load object {object_id}')

        logger.info(f'got {len(objects)} Works, took {elapsed(job_time)} seconds')

        return objects


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', nargs="?", type=str, help="id of the Work authors you want to update")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many Works to update")
    parser.add_argument('--chunk', "-ch", nargs="?", default=10, type=int, help="how many Works to update at once")
    parsed_args = parser.parse_args()

    my_queue = QueueWorkUpdateAuthors()
    my_queue.worker_run(**vars(parsed_args))
