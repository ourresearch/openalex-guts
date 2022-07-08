import argparse
from time import sleep
from time import time

from sqlalchemy import orm
from sqlalchemy import text
from sqlalchemy.orm import selectinload

import models
from app import db
from app import logger
from util import elapsed


class QueueWorkAddEverything:
    def worker_run(self, **kwargs):
        single_id = kwargs.get("id", None)
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)

        if limit is None:
            limit = float("inf")

        if single_id:
            work = QueueWorkAddEverything.fetch_works([single_id])[0]
            work.add_everything()
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
                    logger.info('no queued Works ready to add_everything. waiting...')
                    sleep(5)
                    continue

                works = QueueWorkAddEverything.fetch_works(work_ids)

                for work in works:
                    logger.info(f'running add_everything on {work}')
                    work.add_everything()

                db.session.execute(
                    text('''
                        delete from queue.run_once_work_add_everything q
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
        logger.info("looking for works to add_everything to")

        queue_query = text("""
            with queue_chunk as (
                select work_id
                from queue.run_once_work_add_everything
                where started is null
                order by rand
                limit :chunk
                for update skip locked
            )
            update queue.run_once_work_add_everything q
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
            selectinload(
                models.Work.records
            ).selectinload(
                models.Record.journals
            ).raiseload('*'),
            selectinload(models.Work.records).raiseload('*'),
            selectinload(models.Work.locations).raiseload('*'),
            selectinload(models.Work.journal).raiseload('*'),
            selectinload(models.Work.references).raiseload('*'),
            selectinload(models.Work.references_unmatched).raiseload('*'),
            selectinload(models.Work.mesh),
            selectinload(models.Work.counts_by_year).raiseload('*'),
            selectinload(models.Work.abstract),
            selectinload(models.Work.extra_ids).raiseload('*'),
            selectinload(models.Work.related_works).raiseload('*'),
            selectinload(
                models.Work.affiliations
            ).selectinload(
                models.Affiliation.author
            ).selectinload(
                models.Author.orcids
            ).raiseload('*'),
            selectinload(
                models.Work.affiliations
            ).selectinload(
                models.Affiliation.author
            ).raiseload('*'),
            selectinload(
                models.Work.affiliations
            ).selectinload(
                models.Affiliation.institution
            ).selectinload(
                models.Institution.ror
            ).raiseload('*'),
            selectinload(
                models.Work.affiliations
            ).selectinload(
                models.Affiliation.institution
            ).raiseload('*'),
            selectinload(
                models.Work.concepts
            ).selectinload(
                models.WorkConcept.concept
            ).raiseload('*'),
            selectinload(models.Work.concepts_full).raiseload('*'),
            orm.Load(models.Work).raiseload('*')
        )

    @staticmethod
    def fetch_works(object_ids):
        job_time = time()

        try:
            objects = QueueWorkAddEverything.base_works_query().filter(
                models.Work.paper_id.in_(object_ids)
            ).all()
        except Exception as e:
            logger.exception(f'exception getting records for {object_ids} so trying individually')
            objects = []
            for object_id in object_ids:
                try:
                    objects += QueueWorkAddEverything.base_works_query().filter(
                        models.Work.paper_id == object_id
                    ).all()
                except Exception as e:
                    logger.exception(f'failed to load object {object_id}')

        logger.info(f'got {len(objects)} Works, took {elapsed(job_time)} seconds')

        return objects


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', nargs="?", type=str, help="id of the Work you want to add_everything to")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many Works to update")
    parser.add_argument('--chunk', "-ch", nargs="?", default=10, type=int, help="how many Works to update at once")

    parsed_args = parser.parse_args()

    my_queue = QueueWorkAddEverything()
    my_queue.worker_run(**vars(parsed_args))
