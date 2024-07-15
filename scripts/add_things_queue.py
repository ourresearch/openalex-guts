import argparse
import json
import os
import traceback
from datetime import datetime
from time import time, sleep

import psutil
import requests
from redis.client import Redis
from sqlalchemy import text

import models
from app import REDIS_QUEUE_URL, logger, db
from models import REDIS_ADD_THINGS_QUEUE
from scripts.works_query import base_works_query
from util import work_has_null_author_ids, elapsed, get_openalex_json

_redis = Redis.from_url(REDIS_QUEUE_URL)

CHUNK_SIZE = 50


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip_fast_enqueue', '-s', action='store_true',
                        help='Skip enqueue to fast queue')
    parser.add_argument('--filter', '-f',
                        type=str,
                        action='append',
                        help='OpenAlex API filter(s) to enqueue')
    parser.add_argument('-fname', '--filename', type=str,
                        help='Filename containing DOIs from which to enqueue with priority into add_things queue')
    parser.add_argument('-m', '--method', type=str,
                        help='Methods to run on Work objects', action='append',
                        dest='methods')
    return parser.parse_args()


def enqueue_jobs(work_ids, priority=None, methods=None):
    if methods is None:
        methods = []
    if priority is None:
        priority = time()
    mapping = {json.dumps({'work_id': work_id, 'methods': methods}): priority
               for work_id in work_ids}
    _redis.zadd(REDIS_ADD_THINGS_QUEUE, mapping)


def enqueue_job(work_id, priority=None, methods=None):
    enqueue_jobs([work_id], priority, methods)


def dequeue_chunk(chunk_size):
    items = _redis.zpopmin(REDIS_ADD_THINGS_QUEUE, chunk_size)
    return [json.loads(item[0]) for item in items]


def enqueue_fast_queue(works, priority=None):
    redis_queue_time = time() if priority is None else priority
    redis_queue_mapping = {
        work.paper_id: time() if priority is None else priority
        for work in works if not work_has_null_author_ids(work)
    }
    if redis_queue_mapping:
        _redis.zadd(models.REDIS_WORK_QUEUE, redis_queue_mapping)
    logger.info(
        f'enqueueing works in redis work_store took {elapsed(redis_queue_time, 2)} seconds')


def log_memory_usage():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / (1024 ** 2)

    # Print the memory usage of the current Python process in MB
    logger.info(f"Memory usage (MB): {round(memory_mb, 2)}")


def enqueue_from_api(oa_filters, methods=None):
    for oa_filter in oa_filters:
        logger.info(f'[*] Starting to enqueue using OA filter: {oa_filter}')
        cursor = '*'
        s = requests.session()
        count = 0
        while True:
            try:
                params = {'cursor': cursor,
                          'filter': oa_filter,
                          'per-page': 200,
                          'select': 'id'}
                j = get_openalex_json('https://api.openalex.org/works',
                                      params=params, s=s)
                cursor = j['meta'].get('next_cursor')
                ids = [int(work['id'].split('/W')[-1]) for work in
                       j.get('results', [])]
                if not ids:
                    break
                enqueue_jobs(ids, methods=methods, priority=0)
                count += len(ids)
                logger.info(
                    f'[*] Inserted {count} into add_things queue from filter - {oa_filter}')
            except Exception as e:
                logger.warn(f'[!] Error fetching page for filter - {oa_filter}')
                logger.exception(traceback.format_exception())


def enqueue_txt_file(fname, methods=None):
    with open(fname) as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
        dois = tuple([line for line in lines if line.startswith('10.')])
        doi_work_ids = []
        if dois:
            doi_work_ids = db.session.execute(text(
                'SELECT work_id FROM ins.recordthresher_record WHERE doi IN :dois AND work_id > 0'),
                                          params={'dois': dois}).fetchall()
            doi_work_ids = [r[0] for r in doi_work_ids]
        all_work_ids = [int(item) for item in set(lines) - set(dois)] + doi_work_ids
        enqueue_jobs(all_work_ids, priority=0, methods=methods)


def main():
    args = parse_args()
    if args.filename:
        enqueue_txt_file(args.filename)
        return
    elif args.filter:
        enqueue_from_api(args.filter, methods=args.methods)
        return
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
                fargs = []
                if method_name == 'add_everything':
                    fargs = [True]
                try:
                    method(*fargs)
                    log_memory_usage()
                except Exception as e:
                    logger.info(
                        f'Exception calling {method_name}() on work {work.paper_id}')
                    logger.exception(e)
                    # Re-queue job
                    enqueue_job(work.paper_id, 1e9, job['methods'])
                    errors_count += 1
            total_processed += 1
        now = datetime.now()
        try:
            db.session.commit()
        except Exception as e:
            logger.info(f'Exception while committing db changes, rolling back')
            logger.exception(e)
            db.session.rollback()
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
