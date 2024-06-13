import csv
import json
from time import mktime, gmtime
from argparse import ArgumentParser
from models import REDIS_ADD_THINGS_QUEUE
from app import REDIS_QUEUE_URL

from redis.client import Redis

from util import openalex_works_paginate, normalize_doi

_redis = Redis.from_url(REDIS_QUEUE_URL)


def enqueue_works(work_ids, priority=None, methods=None):
    if methods is None:
        methods = []
    if not priority:
        priority = mktime(gmtime(0))
    mapping = {json.dumps({'work_id': work_id, 'methods': methods}): priority
               for work_id in work_ids}
    _redis.zadd(REDIS_ADD_THINGS_QUEUE, mapping)


def enqueue_work(work_id, priority=None, methods=None):
    enqueue_works([work_id], priority, methods)


def dequeue_chunk(chunk_size):
    items = _redis.zpopmin(REDIS_ADD_THINGS_QUEUE, chunk_size)
    return [json.loads(item) for item in items]


def enqueue_oa_filter(oax_filter):
    print(f'[*] Enqueueing API filter: {oax_filter}')
    count = 0
    for page in openalex_works_paginate(oax_filter, select='id'):
        count += len(page)
        work_ids = [work.get('id') for work in page if work.get('id')]
        if not work_ids:
            continue
        enqueue_works(work_ids)
        print(f'[*] Enqueued {count} works from filter: {oax_filter}')


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--oax_filter', action='append', required=False)
    parser.add_argument('--file', '-f', type=str)
    parser.add_argument('--methods', '-m', nargs='+', type=str)
    return parser.parse_args()


def main():
    args = parse_args()
    f = open(args.file)
    reader = csv.DictReader(f)
    work_ids = []
    for i, line in enumerate(reader):
        work_ids.append(int(line['work_id']))
        if i % 1000 == 0:
            enqueue_works(work_ids, methods=args.methods)
            print(f'Enqueued {len(work_ids)} works')
            work_ids = []


if __name__ == '__main__':
    main()
