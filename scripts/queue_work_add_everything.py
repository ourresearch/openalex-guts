import argparse
import re
from time import sleep, time, mktime, gmtime
from timeit import default_timer as timer

from humanfriendly import format_timespan
from redis import Redis
from sqlalchemy import text

import models
from app import REDIS_QUEUE_URL, db, logger
from models import REDIS_WORK_QUEUE
from scripts.works_query import base_works_query
from util import elapsed, work_has_null_author_ids

_redis = Redis.from_url(REDIS_QUEUE_URL)


class QueueWorkAddEverything:
    def worker_run(self, **kwargs):
        single_id = kwargs.get("id", None)
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)
        partial_update = kwargs.get("partial", False)
        queue_name = self.queue_name(partial_update)
        skip_redis_queue = kwargs.get("skip_redis_queue", False)

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
            if skip_redis_queue:
                logger.info(f"skipping redis fast queue priority for {single_id}")
            else:
                _redis.zadd(REDIS_WORK_QUEUE, {single_id: mktime(gmtime(0))})
                logger.info(f"enqueued {single_id} in redis work_store")
        else:
            num_updated = 0

            while num_updated < limit:
                start_time = time()

                rows = self.fetch_queue_chunk(chunk_size, partial_update=partial_update)
                work_ids = [row[0] for row in rows]

                if not rows:
                    logger.info('no queued Works ready to add_everything. waiting...')
                    sleep(60)
                    continue

                works = QueueWorkAddEverything.fetch_works(work_ids)

                self.add_everything_works(works, rows, partial_update)

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

                if skip_redis_queue:
                    logger.info('skipping redis fast queue priority')
                else:
                    self.prioritize_in_redis_fast_queue(works)

                num_updated += chunk_size
                logger.info(f'processed {len(rows)} Works in {elapsed(start_time, 2)} seconds')

    @staticmethod
    def prioritize_in_redis_fast_queue(works):
        redis_queue_time = time()
        redis_queue_mapping = {
            work.paper_id: mktime(gmtime(0))
            for work in works if not work_has_null_author_ids(work)
        }
        if redis_queue_mapping:
            _redis.zadd(REDIS_WORK_QUEUE, redis_queue_mapping)
        logger.info(f'enqueueing works in redis work_store took {elapsed(redis_queue_time, 2)} seconds')

    @staticmethod
    def add_everything_works(works, rows, partial_update):
        for i, work in enumerate(works):
            logger.info(f'running add_everything on {work}')
            if partial_update and (methods_str := rows[i][1]):
                method_names = re.split('\W', methods_str)
                for method_name in method_names:
                    if not method_name:
                        continue
                    method = getattr(work, method_name, None)
                    if not method:
                        logger.warning(
                            f'method {method_name} does not exist in Work')
                        continue
                    start_time = timer()
                    method()
                    end_time = timer()
                    logger.info(
                        f'finished Work.{method_name} for Work {work.paper_id} in {format_timespan(end_time - start_time)}')
            else:
                work.add_everything(
                    skip_concepts_and_related_works=partial_update)

    @staticmethod
    def queue_name(partial_update):
        return 'run_once_work_add_most_things' if partial_update else 'run_once_work_add_everything'

    @staticmethod
    def fetch_queue_chunk(chunk_size, partial_update=False):
        logger.info("looking for works to add_everything to")

        queue_name = QueueWorkAddEverything.queue_name(partial_update)
        update_sort_order = 'desc' if partial_update else 'asc'
        fields = 'work_id, methods' if partial_update else 'work_id'
        return_fields = 'q.work_id, q.methods' if partial_update else 'q.work_id'
        order = f'order by work_updated {update_sort_order} nulls last, rand'

        queue_query = text(f"""
            with queue_chunk as (
                select {fields}
                from queue.{queue_name}
                where started is null
                {order}
                limit :chunk
                for update skip locked
            )
            update queue.{queue_name} q
            set started = now()
            from queue_chunk
            where q.work_id = queue_chunk.work_id
            returning {return_fields};
        """).bindparams(chunk=chunk_size)

        job_time = time()
        rows = db.engine.execute(queue_query.execution_options(autocommit=True)).all()
        logger.info(f'got {len(rows)} IDs, took {elapsed(job_time)} seconds')

        return rows

    @staticmethod
    def fetch_works(object_ids):
        job_time = time()

        try:
            objects = base_works_query().filter(
                models.Work.paper_id.in_(object_ids)
            ).all()
        except Exception as e:
            logger.exception(f'exception getting records for {object_ids} due to {e} so trying individually')
            objects = []
            for object_id in object_ids:
                try:
                    objects += QueueWorkAddEverything.base_works_query().filter(
                        models.Work.paper_id == object_id
                    ).all()
                except Exception as e:
                    logger.exception(f'failed to load object {object_id} due to {e}')
                    raise

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
    parser.add_argument(
        '--skip-redis-queue', action='store_true',
        help="if set, will skip the step that prioritizes the work in the redis fast queue"
    )

    parsed_args = parser.parse_args()

    my_queue = QueueWorkAddEverything()
    my_queue.worker_run(**vars(parsed_args))
