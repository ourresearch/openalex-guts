import argparse
from time import sleep, time, mktime, gmtime

from redis import Redis
from sqlalchemy import orm
from sqlalchemy import text
from sqlalchemy.orm import selectinload

import models
from app import REDIS_QUEUE_URL
from app import db
from app import logger
from models import REDIS_WORK_QUEUE
from util import elapsed, work_has_null_author_ids

_redis = Redis.from_url(REDIS_QUEUE_URL)


class QueueWorkAddEverything:
    def worker_run(self, **kwargs):
        single_id = kwargs.get("id", None)
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)
        partial_update = kwargs.get("partial", False)
        queue_name = self.queue_name(partial_update)

        if limit is None:
            limit = float("inf")

        if single_id:
            work = QueueWorkAddEverything.fetch_works([single_id])[0]
            work.add_everything(skip_concepts_and_related_works=partial_update)
            db.session.execute(
                text('''
                    insert into queue.work_store (id) values (:work_id)
                    on conflict (id)
                    do update set finished = null
                ''').bindparams(work_id=single_id)
            )
            db.session.commit()
            _redis.zadd(REDIS_WORK_QUEUE, {single_id: mktime(gmtime(0))})
        else:
            num_updated = 0

            while num_updated < limit:
                start_time = time()

                work_ids = self.fetch_queue_chunk(chunk_size, partial_update=partial_update)

                if not work_ids:
                    logger.info('no queued Works ready to add_everything. waiting...')
                    sleep(60)
                    continue

                works = QueueWorkAddEverything.fetch_works(work_ids)

                for work in works:
                    logger.info(f'running add_everything on {work}')
                    work.add_everything(skip_concepts_and_related_works=partial_update)

                db.session.execute(
                    text(f'''
                        delete from queue.{queue_name} q
                        where q.work_id = any(:work_ids)
                    ''').bindparams(work_ids=work_ids)
                )

                logger.info('committing postgres changes')
                commit_start_time = time()
                db.session.commit()
                logger.info(f'commit took {elapsed(commit_start_time, 2)} seconds')

                redis_queue_time = time()
                redis_queue_mapping = {
                    work.paper_id: mktime(gmtime(0))
                    for work in works if not work_has_null_author_ids(work)
                }

                if redis_queue_mapping:
                    _redis.zadd(REDIS_WORK_QUEUE, redis_queue_mapping)
                logger.info(f'enqueueing works in redis work_store took {elapsed(redis_queue_time, 2)} seconds')

                num_updated += chunk_size
                logger.info(f'processed {len(work_ids)} Works in {elapsed(start_time, 2)} seconds')

    @staticmethod
    def queue_name(partial_update):
        return 'run_once_work_add_most_things' if partial_update else 'run_once_work_add_everything'

    @staticmethod
    def fetch_queue_chunk(chunk_size, partial_update=False):
        logger.info("looking for works to add_everything to")

        queue_name = QueueWorkAddEverything.queue_name(partial_update)
        update_sort_order = 'desc' if partial_update else 'asc'

        queue_query = text(f"""
            with queue_chunk as (
                select work_id
                from queue.{queue_name}
                where started is null
                order by work_updated {update_sort_order} nulls last, rand
                limit :chunk
                for update skip locked
            )
            update queue.{queue_name} q
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
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.unpaywall).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.parseland_record).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.child_records).raiseload('*'),
            selectinload(models.Work.records).raiseload('*'),
            selectinload(models.Work.locations).raiseload('*'),
            selectinload(models.Work.journal).raiseload('*'),
            selectinload(models.Work.references).raiseload('*'),
            selectinload(models.Work.references_unmatched).raiseload('*'),
            selectinload(models.Work.mesh),
            selectinload(models.Work.funders).selectinload(models.WorkFunder.funder).raiseload('*'),
            selectinload(models.Work.funders).raiseload('*'),
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
                models.Work.sdg
            ).raiseload('*'),
            selectinload(
                models.Work.concepts
            ).selectinload(
                models.WorkConcept.concept
            ).raiseload('*'),
            selectinload(
                models.Work.topics
            ).selectinload(
                models.WorkTopic.topic
            ).raiseload('*'),
            selectinload(
                models.Work.topics).selectinload(models.WorkTopic.topic
            ).raiseload('*'),
            selectinload(
                models.Work.topics).selectinload(models.WorkTopic.topic).selectinload(models.Topic.subfield
            ).raiseload('*'),
            selectinload(
                models.Work.topics).selectinload(models.WorkTopic.topic).selectinload(models.Topic.field
            ).raiseload('*'),
            selectinload(
                models.Work.topics).selectinload(models.WorkTopic.topic).selectinload(models.Topic.domain
            ).raiseload('*'),
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
    parser.add_argument(
        '--partial', dest='partial', default=False, action='store_true',
        help="skip concept tagging and related work assignment"
    )

    parsed_args = parser.parse_args()

    my_queue = QueueWorkAddEverything()
    my_queue.worker_run(**vars(parsed_args))
