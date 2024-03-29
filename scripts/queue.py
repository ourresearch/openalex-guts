import datetime
import random
from time import sleep
from time import time
import shortuuid
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy import exc
from sqlalchemy.orm import selectinload
from sqlalchemy import insert
from sqlalchemy import delete
from collections import defaultdict
import argparse
import logging
import os
import random
import string

from app import db
from app import logger
from app import MAX_MAG_ID
import models
from models import *  # needed to get the insert tables from the name alone
from util import elapsed
from util import safe_commit

class DbQueue(object):

    def __init__(self, **kwargs):
        self.parsed_vars = {}
        super(DbQueue, self).__init__(**kwargs)


    def update_fn(self, cls, method_name, objects, index=1):
        # we are in a fork!  dispose of our engine.
        # will get a new one automatically. if is pooling, need to do .dispose() instead
        db.engine.dispose()

        start = time()
        num_obj_rows = len(objects)

        if method_name == "new_work_concepts":
            from models.work import call_sagemaker_bulk_lookup_new_work_concepts
            objects = call_sagemaker_bulk_lookup_new_work_concepts(objects)
        else:
            for count, obj in enumerate(objects):
                total_count = count + (num_obj_rows*index)
                start_time = time()
                if obj is None:
                    print("obj is None, so returning")
                    return None
                # logger.info(u"***")
                logger.info("*** #{count} starting {repr}.{method_name}() method".format(
                    count=total_count,
                    repr=obj,
                    method_name=method_name))

                method_to_run = getattr(obj, method_name)
                method_to_run()

                logger.info("finished {repr}.{method_name}(). took {elapsed} seconds".format(
                    repr=obj,
                    method_name=method_name,
                    elapsed=elapsed(start_time, 4)))

        # for count, obj in enumerate(objects):
        #     if hasattr(obj, "insert_dicts"):
        #         print(obj.id, obj.insert_dicts)
        # print(1/0)

        # set to False if want to test without committing anything
        if True:
            keep_going_committing_chunk = True
            # don't use safe_commit for now, want to see the db errors clearly
            if self.myclass == models.Concept and method_name == "clean_metadata":
                db.session.commit()
            if self.myclass == models.Record and method_name == "process_record":
                record_ids = [o.id for o in objects]

                timing_start = time()
                db.session.commit()
                logger.info(f'committing records and works took {elapsed(timing_start, 2)} seconds')

                timing_start = time()
                db.session.execute(
                    text(
                        '''
                        insert into queue.run_once_work_add_everything
                        (work_id, work_updated)
                        (
                            select work_id, updated
                            from ins.recordthresher_record
                            where id = any(:record_ids)
                            and work_id > 0
                        )
                        on conflict do nothing
                        '''
                    ).bindparams(record_ids=record_ids).execution_options(autocommit=True)
                )
                logger.info(f'enqueueing mapped works took {elapsed(timing_start, 2)} seconds')
            if self.myclass == models.Work and method_name in ["add_everything", "add_related_works"]:
                try:
                    db.session.commit()
                except exc.IntegrityError as e:
                    print(f"IntegrityError on commit, so rollback and skipping this chunk.  Error message {e}")
                    db.session.rollback()
                    keep_going_committing_chunk = False

            if keep_going_committing_chunk:
                delete_dict_all_objects = defaultdict(list)
                insert_dict_all_objects = defaultdict(list)
                for count, obj in enumerate(objects):
                    if not hasattr(obj, "delete_dict"):
                        obj.delete_dict = defaultdict(list)

                    if hasattr(obj, "insert_dicts"):
                        for row in obj.insert_dicts:
                            for table_name, insert_dict in row.items():
                                insert_dict_all_objects[table_name] += [insert_dict]
                                if table_name.startswith("Json"):
                                    obj.delete_dict[table_name] += [insert_dict["id"]]

                    for table_name, ids in obj.delete_dict.items():
                        delete_dict_all_objects[table_name] += ids


                start_time = time()
                for table_name, delete_ids in delete_dict_all_objects.items():
                    if table_name == "Work" or table_name == "WorkConceptFull":
                        # print("TO DELETE")
                        # print(delete_ids)
                        my_table = globals()[table_name]
                        db.session.remove()
                        db.session.execute(delete(my_table).where(my_table.paper_id.in_(delete_ids)))
                        db.session.commit()
                        print("delete done")
                    elif table_name.startswith("Json"):
                        # print("TO DELETE")
                        # print(delete_ids)
                        my_table = globals()[table_name]
                        db.session.remove()
                        db.session.execute(delete(my_table).where(my_table.id.in_(delete_ids)))
                        db.session.commit()
                        print("delete done")

                for table_name, all_insert_strings in insert_dict_all_objects.items():
                    # look up the model from the name
                    my_table = globals()[table_name]
                    # print("TO INSERT")
                    # print(my_table)
                    # print(all_insert_strings)
                    db.session.remove()
                    db.session.execute(insert(my_table).values(all_insert_strings))
                    db.session.commit()

                if insert_dict_all_objects:
                    logger.info("insert and commit took {} seconds".format(elapsed(start_time, 2)))
                    # db.session.remove()  # close connection nicely

        return None  # important for if we use this on RQ


    def worker_run(self, **kwargs):
        single_obj_id = kwargs.get("id", None)
        chunk = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", 10)
        worker_name = kwargs.get("name", "myworker")
        run_class = self.myclass
        run_method = kwargs.get("method")

        if single_obj_id:
            limit = 1
            queue_table = None
        else:
            queue_table = self.table_name
            insert_table = None
            if not limit:
                limit = 1000

            if run_method == "add_everything":
                # temp override
                # pubmed ones: 4214798165 4214797353 4214794591
                # crossref ones: 4214778246 4214778318 4214778274
                # text_query_pattern_select = """select 4214704367"""
                text_query_pattern_select = """
                    select paper_id from mid.work
                        where full_updated_date is null
                        order by publication_date desc nulls last, random()
                        limit {chunk}; """
            elif run_method == "add_related_works":
                text_query_pattern_select = """
                select distinct mid.work_concept_for_api_mv.paper_id
                    from mid.work_concept_for_api_mv
                    join mid.work on mid.work.paper_id=mid.work_concept_for_api_mv.paper_id
                    left outer join mid.related_work on mid.related_work.paper_id=mid.work_concept_for_api_mv.paper_id
                    where mid.work_concept_for_api_mv.paper_id > 4200000000
                    and mid.related_work.paper_id is null
                    -- order by random()
                    limit {chunk}; """
            elif run_method in ["store"]:
                text_query_pattern_select = """
                    select {id_field_name}
                        from {queue_table} t1
                        where full_updated_date is not null
                        and full_updated_date > '2022-05-01'
                        and NOT EXISTS (
                           SELECT 1
                           FROM   {insert_table} t2
                           WHERE  (t1.{id_field_name}=t2.id) and ((t1.full_updated_date is not null) and (t1.full_updated_date < t2.updated))
                           and updated > '2022-05-01'
                           )
                        -- order by random()
                        limit {chunk};
                """
                insert_table = self.store_json_insert_tablename
            elif self.myclass == models.Work and run_method=="new_work_concepts":
                text_query_pattern_select = """
                    select distinct mid.work_concept.paper_id from mid.work_concept
                        join mid.work on mid.work.paper_id=mid.work_concept.paper_id
                        join mid.abstract on mid.abstract.paper_id=mid.work_concept.paper_id
                        where mid.work_concept.updated_date is null
                        and mid.abstract.paper_id is not null
                        limit {chunk};
                """
                insert_table = "mid.work_concept"
            elif self.myclass == models.Record:
                # NOT ok to do in parallel because it doesn't set a placeholder, can end up with dups
                text_query_pattern_select = """
                    select id from ins.recordthresher_record
                    where work_id is null
                    order by updated desc nulls last
                    limit {chunk};
                """
            else:
                raise("myclass and method combo not known")

        index = 0
        start_time = time()

        while True:
            # logger.info("the queues query is:\n{}".format(text_query))
            new_loop_start_time = time()

            if single_obj_id:
                object_ids = [single_obj_id]
                # objects = [run_class.query.filter(run_class.id == single_obj_id).first()]
            else:
                text_query_select = text_query_pattern_select.format(
                    chunk=chunk,
                    queue_table=queue_table,
                    insert_table=insert_table,
                    id_field_name=self.id_field_name,
                    MAX_MAG_ID=MAX_MAG_ID
                )
                logger.info("{}: looking for new jobs".format(worker_name))
                job_time = time()
                print(text_query_select)
                row_list = db.session.execute(text(text_query_select)).fetchall()
                logger.info("{}: got ids, took {} seconds".format(worker_name, elapsed(job_time)))
                object_ids = [row[0] for row in row_list]

            job_time = time()
            print(object_ids)
            if (self.myclass == models.Work) and (run_method == "store"):
                try:
                    object_query = db.session.query(models.Work).options(
                         selectinload(models.Work.locations),
                         selectinload(models.Work.journal).raiseload('*'),
                         selectinload(models.Work.references).raiseload('*'),
                         selectinload(models.Work.mesh),
                         selectinload(models.Work.counts_by_year).raiseload('*'),
                         selectinload(models.Work.abstract),
                         selectinload(models.Work.extra_ids).raiseload('*'),
                         selectinload(models.Work.related_works).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
                         selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept).raiseload('*'),
                         orm.Load(models.Work).raiseload('*'))
                    objects = object_query.filter(self.myid.in_(object_ids)).all()
                except Exception as e:
                    print(f"Exception getting objects {e} for {object_ids} so trying individually")
                    objects = []
                    for id in object_ids:
                        try:
                            objects += object_query.filter(self.myid==id).all()
                        except Exception as e:
                            print(f"error: failed on {run_method} {id} with error {e}")
            elif self.myclass == models.Work and run_method.startswith("add_"):
                try:
                    query = db.session.query(models.Work).options(
                         selectinload(models.Work.records).selectinload(models.Record.journals).raiseload('*'),
                         selectinload(models.Work.records).raiseload('*'),
                         selectinload(models.Work.locations).raiseload('*'),
                         selectinload(models.Work.journal).raiseload('*'),
                         selectinload(models.Work.references).raiseload('*'),
                         selectinload(models.Work.references_unmatched).raiseload('*'),
                         selectinload(models.Work.mesh),
                         selectinload(models.Work.counts_by_year).raiseload('*'),
                         selectinload(models.Work.abstract),
                         selectinload(models.Work.extra_ids).raiseload('*'),
                         selectinload(models.Work.related_works).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror).raiseload('*'),
                         selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).raiseload('*'),
                         selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept).raiseload('*'),
                         selectinload(models.Work.concepts_full).raiseload('*'),
                         orm.Load(models.Work).raiseload('*'))
                    objects = query.filter(self.myid.in_(object_ids)).all()
                except Exception as e:
                    print(f"Exception {e} getting records for {object_ids} so trying individually")
                    objects = []
                    for id in object_ids:
                        try:
                            objects += query.filter(self.myid==id).all()
                        except Exception as e:
                            print(f"error: failed on {run_method} {id} with error {e}")

            elif self.myclass == models.Work and run_method=="new_work_concepts":
                q = """select work.paper_id, work.paper_title, work.doc_type,
                        journal.display_name as journal_title, abstract.indexed_abstract
                    from mid.work work
                    left outer join mid.journal journal on journal.journal_id=work.journal_id
                    left outer join mid.abstract abstract on work.paper_id=abstract.paper_id
                    where work.paper_id in ({})
                """.format(",".join(str(paper_id) for paper_id in object_ids))
                objects = db.session.execute(text(q)).fetchall()
            elif self.myclass == models.Record:
                objects = db.session.query(models.Record).options(
                     selectinload(models.Record.work_matches_by_title).raiseload('*'),
                     selectinload(models.Record.work_matches_by_title).selectinload(models.Work.affiliations).raiseload('*'),
                     selectinload(models.Record.work_matches_by_doi).raiseload('*'),
                     selectinload(models.Record.work_matches_by_pmid).raiseload('*'),
                     selectinload(models.Record.journals).raiseload('*'),
                     orm.Load(models.Record).raiseload('*')).filter(self.myid.in_(object_ids)).all()
            elif self.myclass == models.Author:
                objects = db.session.query(models.Author).options(
                     selectinload(models.Author.counts_by_year_papers).raiseload('*'),
                     selectinload(models.Author.counts_by_year_citations).raiseload('*'),
                     selectinload(models.Author.alternative_names),
                     selectinload(models.Author.author_concepts).raiseload('*'),
                     selectinload(models.Author.orcids).selectinload(models.AuthorOrcid.orcid_data),
                     selectinload(models.Author.last_known_institution).selectinload(models.Institution.ror).raiseload('*'),
                     selectinload(models.Author.last_known_institution).raiseload('*'),
                     # orm.Load(models.Author).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                     orm.Load(models.Author).lazyload('*')).filter(self.myid.in_(object_ids)).all()
            elif self.myclass == models.Institution:
                objects = db.session.query(models.Institution).options(
                     selectinload(models.Institution.counts_by_year_papers.raiseload('*')),
                     selectinload(models.Institution.counts_by_year_citations.raiseload('*')),
                     selectinload(models.Institution.ror).raiseload('*'),
                     orm.Load(models.Institution).lazyload('*')).filter(self.myid.in_(object_ids)).all()
                     # selectinload(models.Institution.ror).raiseload('*'),
                     # orm.Load(models.Institution).raiseload('*')).filter(self.myid.in_(object_ids)).all()
            elif self.myclass == models.Concept and (run_method == "calculate_ancestors"):
                objects = db.session.query(models.Concept).options(
                     orm.Load(models.Concept).raiseload('*')).filter(self.myid.in_(object_ids)).all()
            elif self.myclass == models.Concept:
                objects = db.session.query(models.Concept).options(
                     # selectinload(models.Concept.counts_by_year_papers),
                     # selectinload(models.Concept.counts_by_year_citations),
                     selectinload(models.Concept.ancestors),
                     orm.Load(models.Concept).lazyload('*')).filter(self.myid.in_(object_ids)).all()
                     # selectinload(models.Concept.ancestors),
                     # orm.Load(models.Concept).raiseload('*')).filter(self.myid.in_(object_ids)).all()
            elif self.myclass == models.Venue:
                objects = db.session.query(models.Venue).options(
                     selectinload(models.Venue.counts_by_year_papers),
                     selectinload(models.Venue.counts_by_year_citations),
                     orm.Load(models.Venue).raiseload('*')).filter(self.myid.in_(object_ids)).all()
            else:
                objects = db.session.query(self.myclass).options(orm.Load(self.myclass).raiseload('*')).filter(self.myid.in_(object_ids)).all()

            logger.info("{}: got objects in {} seconds".format(worker_name, elapsed(job_time)))

            if not objects:
                logger.info(u"{}: no objects, so sleeping for 5 seconds, then going again".format(worker_name))
                sleep(5)
                continue


            self.update_fn(run_class, run_method, objects, index=index)

            index += 1
            if single_obj_id:
                return
            else:
                self.print_update(new_loop_start_time, chunk, limit, start_time, index)

    def run(self, parsed_args):
        start = time()

        try:
            self.worker_run(**vars(parsed_args))
        except Exception as e:
            logger.fatal('worker_run died with error:', exc_info=True)


        logger.info("finished update in {} seconds".format(elapsed(start)))

        print("fatal error, done run loop, quitting thread")
        return


    def print_update(self, new_loop_start_time, chunk, limit, start_time, index):
        num_items = limit  #let's say have to do the full limit
        num_jobs_remaining = num_items - (index * chunk)
        try:

            jobs_per_hour_this_chunk = chunk / float(elapsed(new_loop_start_time) / 3600)
            predicted_mins_to_finish = round(
                (num_jobs_remaining / float(jobs_per_hour_this_chunk)) * 60,
                1
            )
            logger.info("\n\nWe're doing {} jobs per hour. At this rate, if we had to do everything up to limit, done in {}min".format(
                int(jobs_per_hour_this_chunk),
                predicted_mins_to_finish
            ))
            logger.info("\t{} seconds this loop, {} chunks in {} seconds, {} seconds/chunk average\n".format(
                elapsed(new_loop_start_time),
                index,
                elapsed(start_time),
                round(elapsed(start_time)/float(index), 1)
            ))
        except ZeroDivisionError:
            # logger.info(u"not printing status because divide by zero")
            logger.info(".")


    def run_right_thing(self, parsed_args):
        if parsed_args.id or parsed_args.doi or parsed_args.run:
            if parsed_args.randstart:
                sleep_time = round(random.random(), 2) * 10
                print("Sleeping to randomize start for {} seconds".format(sleep_time))
                sleep(sleep_time)
            self.run(parsed_args)


    @property
    def table_name(self):
        schema = getattr(self.myclass, "__table_args__")["schema"]
        table = getattr(self.myclass, "__tablename__")
        return "{}.{}".format(schema, table)

    @property
    def myclass(self):
        import models

        table = self.parsed_vars.get("table")
        if table == "record":
            myclass = models.Record
        elif table == "work":
            myclass = models.Work
        elif table == "concept":
            myclass = models.Concept
        elif table == "institution":
            myclass = models.Institution
        elif table == "author":
            myclass = models.Author
        elif table == "venue":
            myclass = models.Venue
        return myclass

    @property
    def myid(self):
        import models

        table = self.parsed_vars.get("table")
        if table == "record":
            myid = models.Record.id
        elif table == "work":
            myid = models.Work.paper_id
        elif table == "concept":
            myid = models.Concept.field_of_study_id
        elif table == "institution":
            myid = models.Institution.affiliation_id
        elif table == "venue":
            myid = models.Venue.journal_id
        elif table == "author":
            myid = models.Author.author_id
        return myid

    @property
    def store_json_insert_tablename(self):
        table = self.parsed_vars.get("table")
        return f"mid.json_{table}s"

    @property
    def id_field_name(self):
        myid = "id"
        table = self.parsed_vars.get("table")
        if table == "work":
            myid = "paper_id"
        elif table == "concept":
            myid = "field_of_study_id"
        elif table == "institution":
            myid = "affiliation_id"
        elif table == "venue":
            myid = "journal_id"
        elif table == "author":
            myid = "author_id"
        return myid

    def process_name(self):
        if self.parsed_vars:
            process_name = self.parsed_vars.get("method")
        return process_name



if __name__ == "__main__":
    if os.getenv('OADOI_LOG_SQL'):
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        db.session.configure()

    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--id', nargs="?", type=str, help="id of the one thing you want to update (case sensitive)")
    parser.add_argument('--doi', nargs="?", type=str, help="id of the one thing you want to update (case insensitive)")
    parser.add_argument('--table', nargs="?", type=str, default="record", help="method name to run")
    parser.add_argument('--method', nargs="?", type=str, default="process", help="method name to run")
    parser.add_argument('--run', default=True, action='store_true', help="to run the queues")
    parser.add_argument('--chunk', "-ch", nargs="?", default=5, type=int, help="how many to take off db at once")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many jobs to do")
    parser.add_argument('--name', nargs="?", default="myworker", type=str, help="worker name")
    parser.add_argument('--randstart', default=False, action='store_true', help="randomize the start time")

    parsed_args = parser.parse_args()

    my_queue = DbQueue()
    my_queue.parsed_vars = vars(parsed_args)
    my_queue.run_right_thing(parsed_args)



# unload ($$ select '["' || paper_id || '"],' as line from mid.json_works $$)
# to 's3://unsub-public/loaderio/temp_loaderio_paper_ids.csv'
# credentials CREDS
# ALLOWOVERWRITE
# parallel off
# delimiter as '|'

# head -n 100000 /Users/hpiwowar/Downloads/temp_loaderio_paper_ids.csv000 > /Users/hpiwowar/Downloads/temp_loaderio_paper_ids.csv002

# {
#   "version": 1,
#   "variables": [{
#     "names": ["paper_id"],
#     "values": [

# at bottom
#   ]}
#   ]
# }

# then make it public
