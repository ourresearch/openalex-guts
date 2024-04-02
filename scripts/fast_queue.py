import argparse
from collections import defaultdict
from time import sleep, time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from redis import Redis
from sqlalchemy import orm, text, insert, delete
from sqlalchemy.orm import selectinload

import models
from app import ELASTIC_URL
from app import REDIS_QUEUE_URL
from app import db
from app import logger
from models import REDIS_WORK_QUEUE
from util import elapsed

# test this script locally
# 1. Save environment variables to .env file with: heroku config -s > .env
# 2. Run the script to save an example ID: heroku local:run python -- -m scripts.fast_queue --entity=work --method=store --id=2008120268
# 3. Changes should be reflected in elasticsearch and the api.

_redis = Redis.from_url(REDIS_QUEUE_URL)


def run(**kwargs):
    entity_type = kwargs.get("entity")
    method_name = kwargs.get("method")

    if method_name == "store":
        queue_table = f"queue.{entity_type.lower()}_store"
    else:
        queue_table = f"queue.{method_name.lower()}"

    if single_id := kwargs.get('id'):
        if objects := get_objects(entity_type, [single_id]):
            logger.info(f'found object {objects[0]}')
            bulk_actions = []
            for o in objects:
                record_actions = o.store()
                bulk_actions += [bulk_action for bulk_action in record_actions if bulk_action]
            index_and_merge_object_records(bulk_actions)
            db.session.commit()
        else:
            logger.warn(f'found no object with id {single_id}')
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

                logger.info('committing')
                start_time = time()
                db.session.commit()  # fail loudly for now
                logger.info(f'commit took {elapsed(start_time, 4)}s')

                if method_name == "store" and bulk_actions:
                    logger.info('indexing')
                    start_time = time()
                    index_and_merge_object_records(bulk_actions)
                    logger.info(f'indexing took {elapsed(start_time, 4)}s')

                if entity_type == 'work' and method_name == 'store':
                    log_work_store_time(loop_start, time(), chunk)
                else:
                    finish_object_ids(queue_table, object_ids)

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


def store_json_objects(objects):
    delete_dict_all_objects = defaultdict(list)
    insert_dict_all_objects = defaultdict(list)
    for count, obj in enumerate(objects):
        obj.delete_dict = defaultdict(list)
        for row in obj.insert_dicts:
            for table_name, insert_dict in row.items():
                insert_dict_all_objects[table_name] += [insert_dict]
                obj.delete_dict[table_name] += [insert_dict["id"]]
        for table_name, ids in obj.delete_dict.items():
            delete_dict_all_objects[table_name] += ids

    start_time = time()
    for table_name, delete_ids in delete_dict_all_objects.items():
        my_table = globals()[table_name]
        db.session.remove()
        db.session.execute(delete(my_table).where(my_table.id.in_(delete_ids)))
        db.session.commit()
        print("delete done")
    for table_name, all_insert_strings in insert_dict_all_objects.items():
        my_table = globals()[table_name]
        db.session.remove()
        db.session.execute(insert(my_table).values(all_insert_strings))
        db.session.commit()
    print("insert and commit took {} seconds".format(elapsed(start_time, 2)))


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
    if chunk:
        zadd_start_time = time()
        _redis.zadd(REDIS_WORK_QUEUE, {work_id: time() for work_id in chunk})
        logger.info(f'pushed ids to back of the queue in {elapsed(zadd_start_time, 4)}s')

    logger.info(f'got {len(chunk)} ids from the queue in {elapsed(overall_start_time, 4)}s')
    return chunk


def fetch_queue_chunk_ids_from_pg(queue_table, chunk_size):
    text_query = f"""
              with chunk as (
                  select id
                  from {queue_table}
                  where started is null
                  and (finished is null or finished < now() - '1 hour'::interval)
                  order by
                      finished asc nulls first,
                      rand
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

def finish_object_ids(queue_table, object_ids):
    # logger.info(f'finishing queue chunk')
    start_time = time()

    query_text = f'''
        update {queue_table}
        set finished = now(), started=null
        where id = any(:ids)
    '''

    db.session.execute(text(query_text).bindparams(ids=object_ids))
    db.session.commit()
    # logger.info(f'finished saving finish_objects in {elapsed(start_time, 4)}s')


def get_objects(entity_type, object_ids):
    logger.info(f'getting {len(object_ids)} objects')

    start_time = time()
    if entity_type == "work":
        objects = db.session.query(models.Work).options(
            selectinload(models.Work.stored),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).lazyload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).lazyload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).lazyload(models.Source.merged_into_source).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).lazyload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.journals).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.fulltext).raiseload('*'),
            selectinload(models.Work.records).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.merged_into_source).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.locations).selectinload(models.Location.journal).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.merged_into_source).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.journal).selectinload(models.Source.language_override).raiseload('*'),
            selectinload(models.Work.journal).raiseload('*'),
            selectinload(models.Work.openapc),
            selectinload(models.Work.embeddings),
            selectinload(models.Work.sdg),
            selectinload(models.Work.work_keywords),
            selectinload(models.Work.safety_journals).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.safety_journals).selectinload(models.Source.merged_into_source).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.safety_journals).selectinload(models.Source.merged_into_source).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.safety_journals).selectinload(models.Source.merged_into_source).raiseload('*'),
            selectinload(models.Work.safety_journals).selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
            selectinload(models.Work.safety_journals).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Work.safety_journals).selectinload(models.Source.publisher_entity).raiseload('*'),
            selectinload(models.Work.safety_journals).raiseload('*'),
            selectinload(models.Work.references).raiseload('*'),
            selectinload(models.Work.references_unmatched).raiseload('*'),
            selectinload(models.Work.mesh),
            selectinload(models.Work.doi_ra),
            selectinload(models.Work.retraction_watch),
            selectinload(models.Work.funders).selectinload(models.WorkFunder.funder).raiseload('*'),
            selectinload(models.Work.funders).raiseload('*'),
            selectinload(models.Work.counts),
            selectinload(models.Work.citation_count_2year),
            selectinload(models.Work.counts_by_year).raiseload('*'),
            selectinload(models.Work.abstract),
            selectinload(models.Work.extra_ids).raiseload('*'),
            selectinload(models.Work.related_works).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ancestors).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
            selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept).raiseload('*'),
            selectinload(models.Work.topics).selectinload(models.WorkTopic.topic).raiseload('*'),
            selectinload(models.Work.topics).selectinload(models.WorkTopic.topic).selectinload(models.Topic.subfield).raiseload('*'),
            selectinload(models.Work.topics).selectinload(models.WorkTopic.topic).selectinload(models.Topic.field).raiseload('*'),
            selectinload(models.Work.topics).selectinload(models.WorkTopic.topic).selectinload(models.Topic.domain).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.parseland_record).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.pdf_record).raiseload('*'),
            selectinload(models.Work.records).selectinload(models.Record.child_records).raiseload('*'),
            selectinload(models.Work.related_versions).selectinload(models.WorkRelatedVersion.related_work).raiseload('*'),
            selectinload(models.Work.fulltext),
            orm.Load(models.Work).raiseload('*')
        ).filter(models.Work.paper_id.in_(object_ids)).all()
    elif entity_type == "author":
        objects = db.session.query(models.Author).options(
            selectinload(models.Author.stored),
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
            selectinload(models.Author.orcids).selectinload(models.AuthorOrcid.orcid_data),
            selectinload(models.Author.last_known_institution).selectinload(models.Institution.ancestors).raiseload('*'),
            selectinload(models.Author.last_known_institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Author.last_known_institution).raiseload('*'),
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
            selectinload(models.Source.stored),
            selectinload(models.Source.counts),
            selectinload(models.Source.counts_2year),
            selectinload(models.Source.counts_by_year_papers),
            selectinload(models.Source.counts_by_year_oa_papers),
            selectinload(models.Source.counts_by_year_citations),
            selectinload(models.Source.impact_factor),
            selectinload(models.Source.h_index),
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
            selectinload(models.Institution.repositories).selectinload(models.Source.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Institution.repositories).selectinload(models.Source.institution).raiseload('*'),
            selectinload(models.Institution.repositories).raiseload('*'),
        ).filter(models.Institution.affiliation_id.in_(object_ids)).all()
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
    logger.info(f'got {len(objects)} objects in {elapsed(start_time, 4)}s')
    return objects


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run fast queue.")
    parser.add_argument('--entity', type=str, help="the entity type to run")
    parser.add_argument('--method', type=str, help="the method to run")
    parser.add_argument('--id', nargs="?", type=str, help="id of the one thing you want to update (case sensitive)")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many objects to work on")
    parser.add_argument(
        '--chunk', "-ch", nargs="?", default=100, type=int, help="how many objects to take off the queue at once"
    )

    parsed_args = parser.parse_args()
    run(**vars(parsed_args))
