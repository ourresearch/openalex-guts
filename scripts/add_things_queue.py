import argparse
import json
import os
import traceback
from datetime import datetime
from time import time, sleep

import requests
from redis.client import Redis
from sqlalchemy import text

import models
from app import REDIS_QUEUE_URL, logger, db
from models import REDIS_ADD_THINGS_QUEUE
from scripts.works_query import base_slow_queue_works_query
from util import work_has_null_author_ids, elapsed, get_openalex_json

_redis = Redis.from_url(REDIS_QUEUE_URL)

DEQUEUE_CHUNK_SIZE = 50
SQL_ENQUEUE_CHUNK_SIZE = 100


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip_fast_enqueue', '-sfq', action='store_true',
                        help='Skip enqueue to fast queue')
    parser.add_argument('--filter', '-f',
                        type=str,
                        action='append',
                        help='OpenAlex API filter(s) to enqueue')
    parser.add_argument('--sql_query', '-sql', type=str,
                        help='SQL query to fetch work IDs enqueue')
    parser.add_argument('--fast_queue_priority', '-fqp', type=int, default=None,
                        help='Priority to enqueue into fast queue after completion in this queue')
    parser.add_argument('-fname', '--filename', type=str,
                        help='Filename containing DOIs from which to enqueue with priority into add_things queue')
    parser.add_argument('-m', '--method', type=str,
                        help='Methods to run on Work objects', action='append',
                        dest='methods')
    return parser.parse_args()


def enqueue_jobs(work_ids, priority=None, methods=None,
                 fast_queue_priority=None):
    if methods is None:
        methods = []
    if priority is None:
        priority = time()
    mapping = {json.dumps({'work_id': work_id,
                           'methods': methods,
                           'fast_queue_priority': fast_queue_priority}): priority
               for work_id in work_ids}
    _redis.zadd(REDIS_ADD_THINGS_QUEUE, mapping)


def enqueue_job(work_id, priority=None, methods=None, fast_queue_priority=None):
    enqueue_jobs([work_id], priority, methods, fast_queue_priority)


def dequeue_chunk(chunk_size):
    items = _redis.zpopmin(REDIS_ADD_THINGS_QUEUE, chunk_size)
    return [json.loads(item[0]) for item in items]


def enqueue_fast_queue(works, priority=None):
    redis_queue_time = time() if priority is None else priority
    redis_queue_mapping = {
        work.paper_id: redis_queue_time
        for work in works if not work_has_null_author_ids(work)
    }
    if redis_queue_mapping:
        _redis.zadd(models.REDIS_WORK_QUEUE, redis_queue_mapping)
    logger.info(
        f'enqueueing works in redis work_store took {elapsed(redis_queue_time, 2)} seconds')


def enqueue_from_api(oa_filters, methods=None, fast_queue_priority=None):
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
                enqueue_jobs(ids,
                             methods=methods,
                             priority=0,
                             fast_queue_priority=fast_queue_priority)
                count += len(ids)
                logger.info(
                    f'[*] Inserted {count} into add_things queue from filter - {oa_filter}')
            except Exception as e:
                logger.warn(f'[!] Error fetching page for filter - {oa_filter}')
                logger.exception(traceback.format_exception())


def enqueue_txt_file(fname, methods=None, fast_queue_priority=None):
    with open(fname) as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
        dois = tuple([line for line in lines if line.startswith('10.')])
        doi_work_ids = []
        if dois:
            doi_work_ids = db.session.execute(text(
                'SELECT work_id FROM ins.recordthresher_record WHERE doi IN :dois AND work_id > 0'),
                params={'dois': dois}).fetchall()
            doi_work_ids = [r[0] for r in doi_work_ids]
        all_work_ids = [int(item) if item.isnumeric() else item for item in set(lines) - set(dois)] + doi_work_ids
        enqueue_jobs(all_work_ids, priority=0,
                     methods=methods,
                     fast_queue_priority=fast_queue_priority)


def make_keyset_pagination_query(query):
    query = query.strip(';')
    pagination_column = 'paper_id' if 'SELECT paper_id' in query else 'work_id'
    filter_word = 'WHERE' if 'WHERE' not in query else 'AND'
    pagination_query = f"""
    {query}
    {filter_word} {pagination_column} > :last_work_id
    ORDER BY {pagination_column}
    LIMIT :page_size;
    """
    return pagination_query


def paginate_query(query, last_work_id=0, page_size=SQL_ENQUEUE_CHUNK_SIZE):
    pagination_query = make_keyset_pagination_query(query)
    params = {'last_work_id': last_work_id, 'page_size': page_size}
    result = db.session.execute(text(pagination_query), params)
    return result.mappings().all()


def enqueue_from_sql(sql_query, methods=None, fast_queue_priority=None):
    last_work_id = 0
    has_more = True
    count = 0
    while has_more:
        display_query = make_keyset_pagination_query(sql_query).replace(
            ':last_work_id', str(last_work_id)).replace(':page_size', str(SQL_ENQUEUE_CHUNK_SIZE))
        print(f'Fetching SQL query: {display_query}')
        start = time()
        page = paginate_query(sql_query, last_work_id, SQL_ENQUEUE_CHUNK_SIZE)
        _elapsed = elapsed(start)
        print(f'SQL page query took {_elapsed} seconds')
        work_ids = [list(row.values())[0] for row in page]
        enqueue_jobs(work_ids, methods, fast_queue_priority)
        count += len(work_ids)
        print(f'Successfully enqueued {len(work_ids)} works ({count} total)')
        last_work_id = work_ids[-1]
        has_more = len(work_ids) >= SQL_ENQUEUE_CHUNK_SIZE


def main():
    args = parse_args()
    if args.filename:
        enqueue_txt_file(args.filename)
        return
    elif args.filter:
        enqueue_from_api(args.filter,
                         methods=args.methods,
                         fast_queue_priority=args.fast_queue_priority)
        return
    elif args.sql_query:
        enqueue_from_sql(args.sql_query,
                         methods=args.methods,
                         fast_queue_priority=args.fast_queue_priority)
        return
    total_processed = 0
    errors_count = 0
    start = datetime.now()
    while True:
        try:
            jobs = dequeue_chunk(DEQUEUE_CHUNK_SIZE)
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
        works = base_slow_queue_works_query().filter(
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
                except Exception as e:
                    logger.info(
                        f'Exception calling {method_name}() on work {work.paper_id}')
                    logger.exception(e)
                    # Re-queue job
                    enqueue_job(work.paper_id, 1e9, job['methods'],
                                job.get('fast_queue_priority'))
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
            enqueue_fast_queue(works, priority=job.get('fast_queue_priority'))
        else:
            logger.info(f'Skipping priority enqueue to fast queue')
        hrs_diff = (now - start).total_seconds() / (60 * 60)
        rate = round(total_processed / hrs_diff, 2)
        count_in_queue = _redis.zcard(REDIS_ADD_THINGS_QUEUE)
        logger.info(
            f'Total processed: {total_processed} | Rate: {rate}/hr | Errors: {errors_count} | Count in queue: {count_in_queue} | Last work processed: {works[-1].paper_id}')


if __name__ == '__main__':
    main()
