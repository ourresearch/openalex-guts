import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep, time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from redis import Redis
from sqlalchemy import orm, text, insert, delete
from sqlalchemy.orm import selectinload

import models
from app import ELASTIC_URL, logger
from app import REDIS_QUEUE_URL
from app import db
from app import logger
from models import REDIS_WORK_QUEUE
from scripts.works_query import base_fast_queue_works_query
from util import elapsed

# test this script locally
# 1. Save environment variables to .env file with: heroku config -s > .env
# 2. Run the script to save an example ID: heroku local:run -- python -m scripts.fast_queue --entity=work --method=store --id=2008120268
# 3. Changes should be reflected in elasticsearch and the api.

_redis = Redis.from_url(REDIS_QUEUE_URL)


def run(**kwargs):
    entity_type = kwargs.get("entity")
    method_name = kwargs.get("method")
    queue_table_override = kwargs.get("queue_table")

    if queue_table_override:
        queue_table = queue_table_override
    else:
        queue_table = f"queue.{entity_type.lower()}_store"

    if ids := kwargs.get('id'):
        if objects := get_objects(entity_type, ids):
            logger.info(f'found objects: {[str(obj) for obj in objects]}')
            bulk_actions = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_obj = {executor.submit(o.store): o for o in objects}
                for future in as_completed(future_to_obj):
                    o = future_to_obj[future]
                    try:
                        record_actions = future.result()
                        bulk_actions += [action for action in record_actions if action]
                    except Exception as e:
                        logger.error(f"Error storing object {o}: {e}")
            if kwargs.get('show_difference'):
                show_difference(bulk_actions)
            index_and_merge_object_records(bulk_actions)
            db.session.commit()
        else:
            logger.warn(f'found no objects with ids {ids}')
    else:
        objects_updated = 0
        limit = kwargs.get('limit')
        chunk = kwargs.get('chunk')
        total_count = 0

        while limit is None or objects_updated < limit:
            loop_start = time()
            if object_ids := fetch_queue_chunk_ids(queue_table, chunk):
                objects = get_objects(entity_type, object_ids)
                bulk_actions = []

                for obj in objects:
                    method_start_time = time()
                    total_count += 1

                    print(f"*** #{total_count} starting {obj}.{method_name}() method")

                    method_name = method_name.replace("update_once_", "")
                    method_to_run = getattr(obj, method_name)
                    record_actions = method_to_run()
                    if method_name == "store" and record_actions:
                        for bulk_action in record_actions:
                            bulk_actions.append(bulk_action)

                    logger.info(f">>> finished {obj}.{method_name}(). took {elapsed(method_start_time, 4)} seconds")

                if kwargs.get('show_difference'):
                    show_difference(bulk_actions)

                logger.info('committing')
                start_time = time()
                db.session.commit()  # fail loudly for now
                logger.info(f'commit took {elapsed(start_time, 4)}s')

                if method_name == "store" and bulk_actions:
                    logger.info('indexing')
                    start_time = time()
                    index_and_merge_object_records(bulk_actions)
                    logger.info(f'indexing took {elapsed(start_time, 4)}s')

                if entity_type == 'work' and method_name == 'store' and not queue_table_override:
                    log_work_store_time(loop_start, time(), chunk)
                elif queue_table == 'queue.work_authors_changed_store':
                    remove_object_ids_from_queue(queue_table, object_ids)
                    # push to back of redis queue, ensures the work gets added to fast queue!
                    _redis.zadd(REDIS_WORK_QUEUE, {work_id: time() for work_id in object_ids})
                else:
                    update_object_ids_in_queue(queue_table, object_ids)

                objects_updated += len(objects)

                logger.info(f'processed chunk of {chunk} objects in {elapsed(loop_start, 2)} seconds')
            else:
                logger.info('nothing ready in the queue, waiting 5 seconds...')
                sleep(5)


def log_work_store_time(started, finished, chunk_size):
    text_query = f"""
        insert into log.work_store_batch (started, finished, batch_size)
        values (to_timestamp(:started), to_timestamp(:finished), :batch_size)
    """

    db.engine.execute(text(text_query).bindparams(
        started=started,
        finished=finished,
        batch_size=chunk_size
    ).execution_options(autocommit=True))


def index_and_merge_object_records(bulk_actions):
    es = Elasticsearch([ELASTIC_URL], timeout=30)
    try:
        bulk(es, bulk_actions)
    except BulkIndexError as e:
        for error in e.errors:
            # check if the error is due to a 'not_found' status when trying to delete
            operation, result = next(iter(error.items()))
            if operation == 'delete' and result.get('status') == 404:
                # ignore document not found errors, possibly already deleted
                logger.info(f"ignoring bulk index error document not found: {error}")
            else:
                logger.warn(f"bulk index error occurred: {error}")


def fetch_queue_chunk_ids(queue_table, chunk_size):
    if 'work_store' in queue_table:
        return fetch_queue_chunk_ids_from_redis(queue_table, chunk_size)
    else:
        return fetch_queue_chunk_ids_from_pg(queue_table, chunk_size)


def fetch_queue_chunk_ids_from_redis(queue_table, chunk_size):
    if queue_table != 'queue.work_store':
        return []

    logger.info(f'getting {chunk_size} ids from the queue')
    overall_start_time = time()
    zpop_result = _redis.zpopmin(REDIS_WORK_QUEUE, chunk_size)
    logger.info(f'popped ids from the queue in {elapsed(overall_start_time, 4)}s')

    chunk = [int(t[0]) for t in zpop_result] if zpop_result else []
    logger.info(f"popped ids: {chunk}")
    if chunk:
        zadd_start_time = time()
        _redis.zadd(REDIS_WORK_QUEUE, {work_id: time() for work_id in chunk})
        logger.info(f'pushed ids to back of the queue in {elapsed(zadd_start_time, 4)}s')

    logger.info(f'got {len(chunk)} ids from the queue in {elapsed(overall_start_time, 4)}s')
    return chunk


def fetch_queue_chunk_ids_from_pg(queue_table, chunk_size):
    order_by_clause = "finished asc nulls first, rand"
    if queue_table == "queue.work_authors_changed_store":
        # get new ids first, for when queue is backed up
        order_by_clause = "id desc"

    text_query = f"""
              with chunk as (
                  select id
                  from {queue_table}
                  where started is null
                  and (finished is null or finished < now() - '1 hour'::interval)
                  order by {order_by_clause}
                  limit :chunk
                  for update skip locked
              )
              update {queue_table}
              set started = now()
              from chunk
              where {queue_table}.id = chunk.id
              returning chunk.id;
        """

    logger.info(f'getting {chunk_size} ids from the queue')
    start_time = time()

    ids = [
        row[0] for row in
        db.engine.execute(text(text_query).bindparams(chunk=chunk_size).execution_options(autocommit=True)).all()
    ]

    logger.info(f'got {len(ids)} ids from the queue in {elapsed(start_time, 4)}s')
    logger.info(f'got these ids: {ids}')

    return ids


def update_object_ids_in_queue(queue_table, object_ids):
    query_text = f'''
        update {queue_table}
        set finished = now(), started=null
        where id = any(:ids)
    '''
    db.session.execute(text(query_text).bindparams(ids=object_ids))
    db.session.commit()


def remove_object_ids_from_queue(queue_table, object_ids):
    query_text = f'''
        delete from {queue_table}
        where id = any(:ids)
    '''
    db.session.execute(text(query_text).bindparams(ids=object_ids))
    db.session.commit()


def get_objects(entity_type, object_ids):
    logger.info(f'getting {len(object_ids)} objects')

    start_time = time()
    if entity_type == "work":
        objects = base_fast_queue_works_query().filter(models.Work.paper_id.in_(object_ids)).all()
    elif entity_type == "author":
        objects = db.session.query(models.Author).options(
            selectinload(models.Author.counts),
            selectinload(models.Author.counts_2year),
            selectinload(models.Author.counts_by_year_papers),
            selectinload(models.Author.counts_by_year_oa_papers),
            selectinload(models.Author.counts_by_year_citations),
            selectinload(models.Author.impact_factor),
            selectinload(models.Author.h_index),
            selectinload(models.Author.h_index_2year),
            selectinload(models.Author.i10_index),
            selectinload(models.Author.i10_index_2year),
            selectinload(models.Author.alternative_names),
            selectinload(models.Author.author_concepts),
            selectinload(models.Author.author_topics),
            selectinload(models.Author.orcids).selectinload(models.AuthorOrcid.orcid_data),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.work).selectinload(models.Work.counts),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.work).raiseload('*'),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ancestors).raiseload(
                '*'),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
            orm.Load(models.Author).raiseload('*')
        ).filter(models.Author.author_id.in_(object_ids)).all()
    elif entity_type == "source":
        objects = db.session.query(models.Source).options(
            selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Source.counts),
            selectinload(models.Source.counts_2year),
            selectinload(models.Source.counts_by_year_papers),
            selectinload(models.Source.counts_by_year_oa_papers),
            selectinload(models.Source.counts_by_year_citations),
            selectinload(models.Source.impact_factor),
            selectinload(models.Source.h_index),
            selectinload(models.Source.source_topics),
            selectinload(models.Source.h_index_2year),
            selectinload(models.Source.i10_index),
            selectinload(models.Source.i10_index_2year),
            selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Source.institution).raiseload('*'),
            orm.Load(models.Source).raiseload('*')
        ).filter(models.Source.journal_id.in_(object_ids)).all()
    elif entity_type == "institution":
        objects = db.session.query(models.Institution).options(
            selectinload(models.Institution.repositories).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.merged_into_source).selectinload(models.Source.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.merged_into_source).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Institution.institution_topics),
            selectinload(models.Institution.repositories).selectinload(models.Source.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Institution.repositories).raiseload('*'),
        ).filter(models.Institution.affiliation_id.in_(object_ids)).all()

        # 
    elif entity_type == "concept":
        objects = db.session.query(models.Concept).filter(models.Concept.field_of_study_id.in_(object_ids)).all()
    elif entity_type == "publisher":
        objects = db.session.query(models.Publisher).options(
            selectinload(models.Publisher.parent).raiseload('*'),
        ).filter(models.Publisher.publisher_id.in_(object_ids)).all()
    elif entity_type == "funder":
        objects = db.session.query(models.Funder).filter(models.Funder.funder_id.in_(object_ids)).all()
    elif entity_type == "topic":
        objects = db.session.query(models.Topic).filter(models.Topic.topic_id.in_(object_ids)).all()
    elif entity_type == "domain":
        objects = db.session.query(models.Domain).filter(models.Domain.domain_id.in_(object_ids)).all()
    elif entity_type == "field":
        objects = db.session.query(models.Field).filter(models.Field.field_id.in_(object_ids)).all()
    elif entity_type == "subfield":
        objects = db.session.query(models.Subfield).filter(models.Subfield.subfield_id.in_(object_ids)).all()
    elif entity_type == "sdg":
        objects = db.session.query(models.SDG).filter(models.SDG.sdg_id.in_(object_ids)).all()
    elif entity_type == "work_type":
        objects = db.session.query(models.WorkType).filter(models.WorkType.work_type_id.in_(object_ids)).all()
    elif entity_type == "country":
        objects = db.session.query(models.Country).filter(models.Country.country_id.in_(object_ids)).all()
    elif entity_type == "language":
        objects = db.session.query(models.Language).filter(models.Language.language_id.in_(object_ids)).all()
    elif entity_type == "continent":
        objects = db.session.query(models.Continent).filter(models.Continent.continent_id.in_(object_ids)).all()
    elif entity_type == "institution_type":
        objects = db.session.query(models.InstitutionType).filter(models.InstitutionType.institution_type_id.in_(object_ids)).all()
    elif entity_type == "source_type":
        objects = db.session.query(models.SourceType).filter(models.SourceType.source_type_id.in_(object_ids)).all()
    elif entity_type == "keyword":
        objects = db.session.query(models.Keyword).filter(models.Keyword.keyword_id.in_(object_ids)).all()
    elif entity_type == "license":
        objects = db.session.query(models.License).filter(models.License.license_id.in_(object_ids)).all()
    logger.info(f'got {len(objects)} objects in {elapsed(start_time, 4)}s')
    return objects


def show_difference(bulk_actions):
    es = Elasticsearch([ELASTIC_URL], timeout=30)
    for action in bulk_actions:
        if action.get("op_type") == "delete":
            continue
        # get current record from elasticsearch
        es_record = es.get(index=action["_index"], id=action["_id"], ignore=[404])
        if es_record.get("found"):
            es_record = es_record["_source"]
            # compare the new and old
            diff = compare_records(action["_source"], es_record)
            if diff:
                logger.info(f"diff for id {action['_id']}: {diff}")
            else:
                logger.info(f"no differences found for {action['_id']}")


def compare_records(new_record, old_record):
    new_record_copy = new_record.copy()
    old_record_copy = old_record.copy()
    for key in ["updated_date", "updated", "@timestamp", "@version"]:
        if key in new_record_copy:
            del new_record_copy[key]
        if key in old_record_copy:
            del old_record_copy[key]

    diff = defaultdict(dict)
    for key, value in new_record_copy.items():
        if key not in old_record_copy:
            diff[key]["old"] = None
            diff[key]["new"] = value
        elif value != old_record_copy[key]:
            diff[key]["old"] = old_record_copy[key]
            diff[key]["new"] = value
    return diff


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run fast queue.")
    parser.add_argument('--entity', type=str, help="the entity type to run")
    parser.add_argument('--method', type=str, help="the method to run")
    parser.add_argument('--id', nargs="*", type=str, help="IDs of the objects to update (case sensitive)")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many objects to work on")
    parser.add_argument('--queue_table', type=str, nargs="?", help="the queue table to use, optional")
    parser.add_argument(
        '--chunk', "-ch", nargs="?", default=100, type=int, help="how many objects to take off the queue at once"
    )
    parser.add_argument('--show-difference', "-sd", action="store_true", help="show the difference between the old and new records")

    parsed_args = parser.parse_args()
    run(**vars(parsed_args))
