import argparse
import json
from datetime import datetime
from time import mktime, gmtime, time, sleep

from redis.client import Redis

import models
from app import REDIS_QUEUE_URL, logger, db
from models import REDIS_ADD_THINGS_QUEUE
from scripts.works_query import base_works_query
from util import work_has_null_author_ids, elapsed

_redis = Redis.from_url(REDIS_QUEUE_URL)

CHUNK_SIZE = 50


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip_fast_enqueue', '-s', action='store_true', help='Skip enqueue to fast queue')
    return parser.parse_args()


def enqueue_jobs(work_ids, priority=None, methods=None):
    if methods is None:
        methods = []
    if not priority:
        priority = mktime(gmtime(0))
    mapping = {json.dumps({'work_id': work_id, 'methods': methods}): priority
               for work_id in work_ids}
    _redis.zadd(REDIS_ADD_THINGS_QUEUE, mapping)


def enqueue_job(work_id, priority=None, methods=None):
    enqueue_jobs([work_id], priority, methods)


def dequeue_chunk(chunk_size):
    items = _redis.zpopmin(REDIS_ADD_THINGS_QUEUE, chunk_size)
    return [json.loads(item[0]) for item in items]


def enqueue_fast_queue(works):
    redis_queue_time = time()
    redis_queue_mapping = {
        work.paper_id: mktime(gmtime(0))
        for work in works if not work_has_null_author_ids(work)
    }
    if redis_queue_mapping:
        _redis.zadd(models.REDIS_WORK_QUEUE, redis_queue_mapping)
    logger.info(
        f'enqueueing works in redis work_store took {elapsed(redis_queue_time, 2)} seconds')


def main():
    args = parse_args()
    total_processed = 0
    errors_count = 0
    start = datetime.now()
    while True:
        try:
            jobs = dequeue_chunk(CHUNK_SIZE)
        except Exception as e:
            logger.info('Exception during dequeue, exiting...')
            logger.exception(e)
            break
        if not jobs:
            logger.info(
                f'No jobs found in {REDIS_ADD_THINGS_QUEUE}, sleeping and then checking again')
            sleep(10)
            continue
        jobs_map = {job['work_id']: job for job in jobs}
        works = base_works_query().filter(
            models.Work.paper_id.in_([job['work_id'] for job in jobs])
        ).all()

        for work in works:
            job = jobs_map[work.paper_id]
            if not job['methods']:
                job['methods'] = ['add_everything']
            for method_name in job['methods']:
                method = getattr(work, method_name)
                args = []
                if method_name == 'add_everything':
                    args = [True]
                try:
                    method(*args)
                except Exception as e:
                    logger.info(
                        f'Exception calling {method_name}() on work {work.paper_id}')
                    logger.exception(e)
                    # Re-queue job
                    enqueue_job(work.paper_title, 1e9, job['methods'])
                    errors_count += 1
            total_processed += 1
        now = datetime.now()
        db.session.commit()
        if not args.skip_fast_enqueue:
            enqueue_fast_queue(works)
        else:
            logger.info(f'Skipping priority enqueue to fast queue')
        hrs_diff = (now - start).total_seconds() / (60 * 60)
        rate = round(total_processed / hrs_diff, 2)
        count_in_queue = _redis.zcard(REDIS_ADD_THINGS_QUEUE)
        logger.info(
            f'Total processed: {total_processed} | Rate: {rate}/hr | Errors: {errors_count} | Count in queue: {count_in_queue} | Last work processed: {works[-1].paper_id}')


if __name__ == '__main__':
    main()
