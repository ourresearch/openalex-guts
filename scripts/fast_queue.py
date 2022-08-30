import argparse
from time import sleep, time
from collections import defaultdict

from sqlalchemy import orm, text, insert, delete
from sqlalchemy.orm import selectinload

import models
from app import db
from app import logger
from models.json_store import JsonWorks, JsonAuthors, JsonConcepts, JsonInstitutions, JsonVenues
from util import elapsed


def run(**kwargs):
    entity_type = kwargs.get("entity")
    method_name = kwargs.get("method")

    if method_name == "store":
        queue_table = f"queue.{entity_type.lower()}_store"
    else:
        queue_table = f"queue.{method_name.lower()}"

    if single_id := kwargs.get('id'):
        if objects := get_objects(entity_type, [single_id]):
            [o.store() for o in objects]
            logger.info(f'found object {objects[0]}')
            store_json_objects(objects)
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

                for obj in objects:
                    method_start_time = time()
                    total_count += 1

                    print(f"*** #{total_count} starting {obj}.{method_name}() method")

                    method_name = method_name.replace("update_once_", "")
                    method_to_run = getattr(obj, method_name)
                    method_to_run()

                    print(f">>> finished {obj}.{method_name}(). took {elapsed(method_start_time, 4)} seconds")

                # print(1/0)
                logger.info('committing')
                start_time = time()

                db.session.commit() # fail loudly for now
                if method_name == "store":
                    store_json_objects(objects)
                logger.info(f'commit took {elapsed(start_time, 4)}s')

                finish_object_ids(queue_table, object_ids)
                objects_updated += len(objects)

                logger.info(f'processed chunk of {chunk} objects in {elapsed(loop_start, 2)} seconds')
            else:
                logger.info('nothing ready in the queue, waiting 5 seconds...')
                sleep(5)


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
    text_query = f"""
          with chunk as (
              select id
              from {queue_table}
              where started is null
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
            selectinload(models.Work.records).selectinload(models.Record.journals).raiseload('*'),
            selectinload(models.Work.records).raiseload('*'),
            selectinload(models.Work.locations),
            selectinload(models.Work.journal).raiseload('*'),
            selectinload(models.Work.references).raiseload('*'),
            selectinload(models.Work.references_unmatched).raiseload('*'),
            selectinload(models.Work.mesh),
            selectinload(models.Work.counts),
            selectinload(models.Work.counts_by_year).raiseload('*'),
            selectinload(models.Work.abstract),
            selectinload(models.Work.extra_ids).raiseload('*'),
            selectinload(models.Work.related_works).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
            selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept).raiseload('*'),
            orm.Load(models.Work).raiseload('*')
        ).filter(models.Work.paper_id.in_(object_ids)).all()
    elif entity_type == "author":
        objects = db.session.query(models.Author).options(
            selectinload(models.Author.stored),
            selectinload(models.Author.counts),
            selectinload(models.Author.counts_by_year_papers),
            selectinload(models.Author.counts_by_year_citations),
            selectinload(models.Author.alternative_names),
            selectinload(models.Author.author_concepts),
            selectinload(models.Author.orcids).selectinload(models.AuthorOrcid.orcid_data),
            selectinload(models.Author.last_known_institution).selectinload(models.Institution.ror).raiseload('*'),
            selectinload(models.Author.last_known_institution).raiseload('*'),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.work).selectinload(models.Work.counts),
            selectinload(models.Author.affiliations).selectinload(models.Affiliation.work).raiseload('*'),
            orm.Load(models.Author).raiseload('*')
        ).filter(models.Author.author_id.in_(object_ids)).all()
    elif entity_type == "venue":
        objects = db.session.query(models.Venue).options(
             selectinload(models.Venue.stored),
             selectinload(models.Venue.counts),
             selectinload(models.Venue.counts_by_year_papers),
             selectinload(models.Venue.counts_by_year_citations),
             orm.Load(models.Venue).raiseload('*')
        ).filter(models.Venue.journal_id.in_(object_ids)).all()
    elif entity_type == "institution":
        objects = db.session.query(models.Institution).filter(models.Institution.affiliation_id.in_(object_ids)).all()
    elif entity_type == "concept":
        objects = db.session.query(models.Concept).filter(models.Concept.field_of_study_id.in_(object_ids)).all()
    logger.info(f'got {len(objects)} objects in {elapsed(start_time, 4)}s')
    return objects



# python -m scripts.fast_queue --entity=work --method=add_everything --limit=3
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

# for reference, currently running queues

# get those that don't have the new concepts yet
# insert into queue.update_once_work_concepts (id) (select distinct paper_id from mid.work_concept where uses_newest_algorithm=False and algorithm_version=2)

# got all those that don't have related works already
# insert into queue.update_once_work_related_works (id) (select distinct paper_id from mid.work w where not exists (select 1 from mid.related_work rw where rw.paper_id=w.paper_id))

# affiliations with paper_id > 4200 already updated
# insert into queue.update_once_affiliation_institutions (id) (select distinct paper_id from mid.affiliation where original_affiliation is not null and paper_id<4200000000)
