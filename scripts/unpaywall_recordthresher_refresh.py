import argparse
import json
import traceback
from datetime import datetime

import requests
from redis import Redis
from sqlalchemy import text

from app import REDIS_QUEUE_URL, db
from scripts.add_things_queue import enqueue_jobs
from util import openalex_works_paginate, normalize_doi

REDIS_UNPAYWALL_REFRESH_QUEUE = 'queue:unpaywall_refresh'

redis = Redis.from_url(REDIS_QUEUE_URL)

UPW_SESSION = requests.session()


def get_upw_response(doi):
    url = f'https://api.unpaywall.org/v2/{doi}?email=team@ourrsearch.org'
    r = UPW_SESSION.get(url)
    r.raise_for_status()
    return r.json()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filter',
                        help='Filter to add to unpaywall recordthresher fields refresh queue',
                        action='append')
    return parser.parse_args()


def enqueue_oa_filter(oax_filter):
    print(f'[*] Enqueueing API filter: {oax_filter}')
    count = 0
    for page in openalex_works_paginate(oax_filter, select='doi'):
        count += len(page)
        dois = tuple([normalize_doi(work['doi']) for work in page if work.get('doi')])
        if not dois:
            continue
        recordthresher_ids = db.session.execute(text(
            'SELECT id FROM ins.recordthresher_record WHERE doi IN :dois AND work_id > 0'), params={'dois': dois}).fetchall()
        recordthresher_ids = [r[0] for r in recordthresher_ids]
        redis.sadd(REDIS_UNPAYWALL_REFRESH_QUEUE, *recordthresher_ids)
        print(f'[*] Enqueued {count} works from filter: {oax_filter}')


def refresh_from_queue():
    count = 0
    start = datetime.now()
    work_ids_batch = []
    while True:
        recordthresher_id = redis.spop(REDIS_UNPAYWALL_REFRESH_QUEUE)
        if recordthresher_id is None:
            break
        doi, work_id = db.session.execute(
            'SELECT doi, work_id FROM ins.recordthresher_record WHERE id = :id',
            {'id': recordthresher_id.decode()}).fetchone()
        if not doi or work_id < 0:
            print(f'Work ID or DOI missing for recordthresher id: {recordthresher_id.decode()}, skipping')
            continue
        try:
            upw_response = get_upw_response(doi)
        except Exception as e:
            print(f"Error fetching Unpaywall response for DOI: {doi}")
            print(traceback.format_exc())
            continue
        best_oa_location = (upw_response.get('best_oa_location', {}) or {})
        params = {'now': datetime.now(),
                  'oa_status': upw_response.get('oa_status'),
                  'is_paratext': upw_response.get('is_paratext'),
                  'best_oa_url': best_oa_location.get('url'),
                  'best_oa_version': best_oa_location.get('version'),
                  'best_oa_license': best_oa_location.get('license'),
                  'issn_l': upw_response.get('journal_issn_l'),
                  'oa_locations_json': json.dumps(
                      upw_response.get('oa_locations')),
                  'id': recordthresher_id.decode()}
        db.session.execute(
            'UPDATE ins.unpaywall_recordthresher_fields SET updated = :now, oa_status = :oa_status, is_paratext = :is_paratext, '
            'best_oa_location_url = :best_oa_url, best_oa_location_version = :best_oa_version,  '
            'best_oa_location_license = :best_oa_license, issn_l = :issn_l, oa_locations_json = :oa_locations_json WHERE recordthresher_id = :id',
            params)
        count += 1
        work_ids_batch.append(work_id)
        if count % 50 == 0:
            db.session.commit()
            hrs_running = (datetime.now() - start).total_seconds() / (60 * 60)
            rate = round(count / hrs_running, 2)
            q_size = redis.scard(REDIS_UNPAYWALL_REFRESH_QUEUE)
            enqueue_jobs(work_ids_batch)
            print(
                f'Updated count: {count} | Rate: {rate}/hr | Queue size: {q_size} | Last DOI: {doi}')


if __name__ == '__main__':
    args = parse_args()
    if args.filter:
        for _f in args.filter:
            enqueue_oa_filter(_f)
    else:
        refresh_from_queue()
