import argparse
import json
import time
import traceback
from datetime import datetime

import requests
from redis import Redis
from sqlalchemy import text

from app import REDIS_QUEUE_URL, db, unpaywall_db_engine
from scripts.add_things_queue import enqueue_jobs
from util import openalex_works_paginate, normalize_doi

REDIS_UNPAYWALL_REFRESH_QUEUE = 'queue:unpaywall_refresh'

redis = Redis.from_url(REDIS_QUEUE_URL)

DB_CONN = unpaywall_db_engine.engine.connect()


def get_upw_responses(dois):
    placeholders = ', '.join(['(:doi{})'.format(i) for i in range(len(dois))])

    # SQL query with CTE and LEFT JOIN
    query = text("""
    WITH input_dois AS (
        SELECT * FROM (VALUES {}) AS v(doi)
    )
    SELECT
        input_dois.doi,
        pub.response_jsonb
    FROM
        input_dois
    LEFT JOIN
        pub
    ON
        input_dois.doi = pub.id;
    """.format(placeholders))

    # Creating a dictionary of parameters
    params = {'doi{}'.format(i): doi for i, doi in enumerate(dois)}

    # Assuming DB_CONN is your SQLAlchemy connection
    rows = DB_CONN.execute(query, params)

    # Fetch all results
    return rows.mappings().all()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filter',
                        help='Filter to add to unpaywall recordthresher fields refresh queue',
                        action='append')
    parser.add_argument('-fname', '--filename', help='Filename to enqueue DOIs from', type=str)
    parser.add_argument('-doi',
                        help='Single DOI to refresh for debugging purposes',
                        type=str)
    return parser.parse_args()


def enqueue_oa_filter(oax_filter):
    print(f'[*] Enqueueing API filter: {oax_filter}')
    count = 0
    for page in openalex_works_paginate(oax_filter, select='doi'):
        count += len(page)
        dois = tuple(
            [normalize_doi(work['doi']) for work in page if work.get('doi')])
        if not dois:
            continue
        recordthresher_ids = db.session.execute(text(
            'SELECT id FROM ins.recordthresher_record WHERE doi IN :dois AND work_id > 0'),
            params={'dois': dois}).fetchall()
        recordthresher_ids = [r[0] for r in recordthresher_ids]
        redis_queue_mapping = {
            recordthresher_id: 0 for recordthresher_id in
            recordthresher_ids
        }
        redis.zadd(REDIS_UNPAYWALL_REFRESH_QUEUE, redis_queue_mapping)
        print(f'[*] Enqueued {count} works from filter: {oax_filter}')


def enqueue_dois_txt_file(fname):
    with open(fname) as f:
        dois = tuple([line.strip() for line in f.readlines() if line.strip()])
        recordthresher_ids = db.session.execute(text('SELECT id FROM ins.recordthresher_record WHERE doi IN :dois AND work_id > 0'),
                                                params={'dois': dois}).fetchall()
        recordthresher_ids = [r[0] for r in recordthresher_ids]
        redis_queue_mapping = {
            recordthresher_id: 0 for recordthresher_id in
            recordthresher_ids
        }
        if redis_queue_mapping:
            redis.zadd(REDIS_UNPAYWALL_REFRESH_QUEUE, redis_queue_mapping)
            print(f'[*] Enqueued {len(dois)} works from {fname}')
        else:
            print('[!] No recordthresher_ids found for DOIs')


def upsert_in_db(upw_response, recordthresher_id: bytes, doi: str):
    best_oa_location = (upw_response.get('best_oa_location', {}) or {})
    params = {'now': datetime.now(),
              'doi': doi,
              'oa_status': upw_response.get('oa_status'),
              'is_paratext': upw_response.get('is_paratext'),
              'best_oa_url': best_oa_location.get('url'),
              'best_oa_version': best_oa_location.get('version'),
              'best_oa_license': best_oa_location.get('license'),
              'issn_l': upw_response.get('journal_issn_l'),
              'oa_locations_json': json.dumps(
                  upw_response.get('oa_locations')),
              'id': recordthresher_id.decode()}
    sql = text('''
        INSERT INTO ins.unpaywall_recordthresher_fields (recordthresher_id, doi, updated, oa_status, is_paratext,
                                                         best_oa_location_url, best_oa_location_version,
                                                         best_oa_location_license, issn_l, oa_locations_json)
        VALUES (:id, :doi, :now, :oa_status, :is_paratext, :best_oa_url, :best_oa_version,
                :best_oa_license, :issn_l, :oa_locations_json)
        ON CONFLICT (recordthresher_id)
        DO UPDATE SET updated = :now,
                      doi = :doi,
                      oa_status = :oa_status,
                      is_paratext = :is_paratext,
                      best_oa_location_url = :best_oa_url,
                      best_oa_location_version = :best_oa_version,
                      best_oa_location_license = :best_oa_license,
                      issn_l = :issn_l,
                      oa_locations_json = :oa_locations_json;
    ''')

    db.session.execute(sql, params)


def refresh_single(doi):
    recordthresher_id, work_id = db.session.execute(
        'SELECT id, work_id FROM ins.recordthresher_record WHERE doi = :doi AND record_type = :record_type AND work_id > 0',
        {'doi': doi, 'record_type': 'crossref_doi'}).fetchone()
    upw_responses = get_upw_responses([doi])
    upsert_in_db(upw_responses[0]['response_jsonb'], recordthresher_id.encode(), doi)
    enqueue_jobs([work_id], priority=0)
    db.session.commit()


def refresh_from_queue():
    count = 0
    start = datetime.now()
    work_ids_batch = []
    dois_batch = {}
    chunk_size = 50
    while True:
        recordthresher_ids = redis.zpopmin(REDIS_UNPAYWALL_REFRESH_QUEUE,
                                           chunk_size)
        if not recordthresher_ids:
            print('Queue is empty, trying again shortly...')
            time.sleep(10)
            continue
        recordthresher_ids = [recordthresher_id[0] for recordthresher_id in
                              recordthresher_ids]
        for recordthresher_id in recordthresher_ids:
            if recordthresher_id is None:
                break
            doi, work_id = db.session.execute(
                'SELECT doi, work_id FROM ins.recordthresher_record WHERE id = :id',
                {'id': recordthresher_id.decode()}).fetchone()
            if not doi or work_id < 0:
                print(
                    f'Work ID or DOI missing for recordthresher id: {recordthresher_id.decode()}, skipping')
                continue
            if not doi or not doi.strip():
                continue
            dois_batch[doi] = recordthresher_id
            # update_in_db(upw_response, recordthresher_id.decode())
            work_ids_batch.append(work_id)
            count += 1
        upw_responses = get_upw_responses(list(dois_batch.keys()))
        for upw_response in upw_responses:
            if upw_response['response_jsonb']:
                upsert_in_db(upw_response['response_jsonb'],
                             dois_batch[upw_response['doi']],
                             upw_response['doi'])
        db.session.commit()
        hrs_running = (datetime.now() - start).total_seconds() / (60 * 60)
        rate = round(count / hrs_running, 2)
        q_size = redis.zcard(REDIS_UNPAYWALL_REFRESH_QUEUE)
        enqueue_jobs(work_ids_batch)
        work_ids_batch.clear()
        dois_batch.clear()
        print(
            f'Upserted count: {count} | Rate: {rate}/hr | Queue size: {q_size} | Last DOI: {doi}')


if __name__ == '__main__':
    args = parse_args()
    if args.filename:
        enqueue_dois_txt_file(args.filename)
    elif args.doi:
        refresh_single(args.doi)
    elif args.filter:
        for _f in args.filter:
            enqueue_oa_filter(_f)
    else:
        refresh_from_queue()
