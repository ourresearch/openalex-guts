import argparse
import datetime
import hashlib
import json
import uuid
from multiprocessing import Pool
from time import sleep, time

import requests
import shortuuid
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from app import db
from app import logger
from models import Record
from util import elapsed


def _parseland_api_url(doi):
    return f'https://parseland.herokuapp.com/parse-publisher?doi={doi}'


def _record_id(doi):
    return shortuuid.encode(
        uuid.UUID(bytes=hashlib.sha256(f'parseland:{doi}'.encode('utf-8')).digest()[0:16])
    )


def _get_parseland_dict(work):
    logger.info(f'making record for {work}')
    try:
        response = requests.get(_parseland_api_url(work['doi']), verify=False)
        if response.ok:
            response = response.json()['message']
            authors = response.get('authors')
            pl_record = Record(
                id=_record_id(work['doi']),
                record_type='crossref_parseland',
                authors=(authors and json.dumps(authors)) or '[]',
                published_date=response.get('published_date'),
                genre=response.get('genre'),
                abstract=response.get('abstract'),
                doi=work['doi'],
                work_id=-1,
                updated=datetime.datetime.utcnow().isoformat()
            )
            return pl_record
        else:
            logger.error(f"no response for {work['doi']}")
            return None
    except:
        logger.exception()
        return None


def record_dict(rt_record):
    row_dict = {}

    for column in rt_record.__table__.columns:
        row_dict[column.name] = getattr(rt_record, column.name)

    return row_dict


class QueueMakeParselandRTRecords:
    @staticmethod
    def queue_name():
        return 'parseland_record_store'

    def worker_run(self, **kwargs):
        chunk_size = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", None)

        if limit is None:
            limit = float("inf")

        num_updated = 0

        while num_updated < limit:
            start_time = time()

            works = self.fetch_queue_chunk(chunk_size)

            if not works:
                logger.info('no queued records ready to fetch. waiting...')
                sleep(60)
                continue

            with Pool(5) as pool:
                insert_values = pool.map(_get_parseland_dict, works)
                insert_values = [v for v in insert_values if v]

            if insert_values:
                logger.info(f'inserting {len(insert_values)} records')
                result_proxy = db.session.execute(
                    insert(Record).values(
                        [record_dict(r) for r in insert_values]
                    ).on_conflict_do_nothing(index_elements=['id'])
                )
                logger.info(f"{result_proxy.rowcount} records were new")
            else:
                logger.info('inserting no records')

            db.session.execute(
                text(f'''
                    delete from queue.{QueueMakeParselandRTRecords.queue_name()} q
                    where q.id = any(:work_ids)
                ''').bindparams(work_ids=[w['id'] for w in works])
            )

            logger.info('committing changes')
            commit_start_time = time()
            db.session.commit()
            logger.info(f'commit took {elapsed(commit_start_time, 2)} seconds')

            logger.info(f'processed {len(works)} works in {elapsed(start_time, 2)} seconds')
            num_updated += len(works)

    @staticmethod
    def fetch_queue_chunk(chunk_size):
        logger.info("looking for records to fetch parseland records for")

        queue_name = QueueMakeParselandRTRecords.queue_name()

        queue_query = text(f"""
            with queue_chunk as (
                select id, doi
                from queue.{queue_name}
                where started is null
                order by rand
                limit :chunk
                for update skip locked
            )
            update queue.{queue_name} q
            set started = now()
            from queue_chunk
            where q.id = queue_chunk.id
            returning q.id, q.doi;
        """).bindparams(chunk=chunk_size)

        job_time = time()
        id_list = [
            {'id': r[0], 'doi': r[1]}
            for r in db.engine.execute(queue_query.execution_options(autocommit=True)).all()
        ]
        logger.info(f'got {len(id_list)} IDs, took {elapsed(job_time)} seconds')

        return id_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', nargs="?", type=str, help="id of the Work you want to add_everything to")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many Works to update")
    parser.add_argument('--chunk', "-ch", nargs="?", default=10, type=int, help="how many Works to update at once")

    parsed_args = parser.parse_args()

    my_queue = QueueMakeParselandRTRecords()
    my_queue.worker_run(**vars(parsed_args))
