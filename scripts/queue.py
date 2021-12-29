import datetime
import random
from time import sleep
from time import time
import shortuuid
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
from sqlalchemy import MetaData
from sqlalchemy import Table
from collections import defaultdict
import argparse
import logging
import os

from app import db
from app import logger
from app import MAX_MAG_ID
import models
from util import elapsed




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
                start_time = time()
                if obj is None:
                    return None
                # logger.info(u"***")
                logger.info("*** #{count} starting {repr}.{method_name}() method".format(
                    count=count + (num_obj_rows*index),
                    repr=obj,
                    method_name=method_name))

                if method_name in ["store_work_high", "store_work_low", "store_author_high", "store_author_low"]:
                    method_name = "store"
                method_to_run = getattr(obj, method_name)
                method_to_run()

                logger.info("finished {repr}.{method_name}(). took {elapsed} seconds".format(
                    repr=obj,
                    method_name=method_name,
                    elapsed=elapsed(start_time, 4)))

        if self.myclass == models.Concept and method_name=="clean_metadata":
            db.session.commit()

        insert_dict_all_objects = defaultdict(list)
        for count, obj in enumerate(objects):
            if hasattr(obj, "insert_dicts"):
                for row in obj.insert_dicts:
                    for table_name, insert_string in row.items():
                        insert_dict_all_objects[table_name] += [insert_string]

        start_time = time()
        metadata = MetaData(schema="mid")
        metadata.reflect(db.engine, only=[table_name.split(".")[1]])
        my_table = Table(table_name.split(".")[1], metadata, schema="mid", autoload=True, autoload_with=db.engine)

        for table_name, all_insert_strings in insert_dict_all_objects.items():
            fields = obj.get_insert_dict_fieldnames(table_name)

            db.session.execute(my_table.insert(), all_insert_strings)
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
            if run_method in ["store"]:
                text_query_pattern_select = """
                    select {id_field_name} from {queue_table}
                        where {id_field_name} not in
                            (select id from {insert_table})
                        and {id_field_name} < {MAX_MAG_ID}                            
                        order by random()
                        limit {chunk};
                """
                insert_table = self.store_json_insert_tablename
            elif run_method == "store_author_high":
                text_query_pattern_select = """
                    select {id_field_name} from {queue_table}
                        where {id_field_name} not in
                            (select id from {insert_table})
                        and author_id > 2692399391
                        and author_id < {MAX_MAG_ID}
                        order by random()
                        limit {chunk};
                """
                insert_table = self.store_json_insert_tablename
            elif run_method == "store_author_low":
                text_query_pattern_select = """
                    select {id_field_name} from {queue_table}
                        where {id_field_name} not in
                            (select id from {insert_table})
                        and author_id <= 2692399391
                        order by random()
                        limit {chunk};
                """
                insert_table = self.store_json_insert_tablename
            elif run_method == "store_work_high":
                text_query_pattern_select = """
                    select {id_field_name} from {queue_table}
                        where {id_field_name} not in
                            (select id from {insert_table})
                        and paper_id > 2331496286
                        and paper_id < {MAX_MAG_ID}
                        order by random()
                        limit {chunk};
                """
                insert_table = self.store_json_insert_tablename
            elif run_method == "store_work_low":
                text_query_pattern_select = """
                    select {id_field_name} from {queue_table}
                        where {id_field_name} not in
                            (select id from {insert_table})
                        and paper_id <= 2331496286
                        order by random()
                        limit {chunk};
                """
                insert_table = self.store_json_insert_tablename
            elif self.myclass == models.Work and run_method=="new_work_concepts":
                text_query_pattern_select = """
                    select {id_field_name} from mid.works_not_done_yet
                        where {id_field_name} not in
                            (select {id_field_name} from {insert_table})
                        order by random()
                        limit {chunk};
                """
                insert_table = "mid.new_work_concepts"
            elif self.myclass == models.Record:
                # text_query_pattern_select = """
                #     select {id_field_name}
                #     from ins.recordthresher_record
                #     where
                #     updated >= '2021-11-30'
                #     order by random()
                #     limit {chunk};
                # """
               text_query_pattern_select = """
                    select record_id from mid.record_match 
                    join ins.recordthresher_record on ins.recordthresher_record.id=mid.record_match.record_id
                    where matching_work_id is null
                    and doi is null
                    order by random()
                    limit {chunk};
                """
            elif self.myclass == models.Concept and run_method=="store_ancestors":
                text_query_pattern_select = """
                    select field_of_study_id from mid.concept
                        where 
                        field_of_study_id not in (select id from mid.concept_ancestor)
                        and field_of_study_id in (select child_field_of_study_id from legacy.mag_advanced_field_of_study_children)                            
                        order by random()
                        limit {chunk};
                """
            elif self.myclass == models.Concept and run_method=="save_wiki":
                # text_query_pattern_select = """
                #     select field_of_study_id from mid.concept
                #         where
                #         field_of_study_id not in (select field_of_study_id from ins.wiki_concept)
                #         and paper_count >= 400
                #         order by random()
                #         limit {chunk};
                # """
                text_query_pattern_select = """
                    select field_of_study_id from mid.concept
                        where
                        field_of_study_id in (select field_of_study_id from ins.wiki_concept 
                                                where (wikidata_id != 'None') 
                                                    and (wikidata_super is null) 
                                                    and (wikidata_id is not null)
                                                    and (is_active_concept = true))
                        and field_of_study_id not in (select field_of_study_id from ins.wiki_concept 
                                                where (wikidata_super is not null) )
                        order by random()
                        limit {chunk};
                    """
            elif self.myclass == models.Concept and run_method=="clean_metadata":
                text_query_pattern_select = """
                    select field_of_study_id from mid.concept_metadata
                        where
                        updated is null
                        order by random()
                        limit {chunk};
                """
            elif self.myclass == models.Institution and run_method=="save_wiki":
                # text_query_pattern_select = """
                #     select affiliation_id from mid.institution
                #         where
                #         affiliation_id not in (select affiliation_id from ins.wiki_institution)
                #         order by random()
                #         limit {chunk};
                # """
                text_query_pattern_select = """
                    select affiliation_id from mid.institution
                        where
                        affiliation_id in (select affiliation_id from ins.wiki_institution where (wikidata_id != 'None') and (wikidata_super is null) and (wikidata_id is not null))
                        order by random()
                        limit {chunk};
                    """
            else:
                raise("myclass and method combo not known")

        index = 0
        start_time = time()
        big_chunk = 10000
        while True:
            text_query_select = text_query_pattern_select.format(
                chunk=big_chunk,
                queue_table=queue_table,
                insert_table=insert_table,
                id_field_name=self.id_field_name,
                MAX_MAG_ID=MAX_MAG_ID
            )
            # logger.info("the queues query is:\n{}".format(text_query))

            if single_obj_id:
                single_obj_id = normalize_doi(single_obj_id)
                objects = [run_class.query.filter(run_class.id == single_obj_id).first()]
            else:
                logger.info("{}: looking for new jobs".format(worker_name))
                job_time = time()
                print(text_query_select)
                row_list = db.session.execute(text(text_query_select)).fetchall()
                logger.info("{}: got ids, took {} seconds".format(worker_name, elapsed(job_time)))

                number_of_smaller_chunks = int(big_chunk/chunk)
                for chunk_number in range(0, number_of_smaller_chunks):
                    new_loop_start_time = time()

                    object_ids = [row[0] for row in row_list[(chunk*chunk_number):(chunk*(chunk_number+1))]]

                    job_time = time()
                    print(object_ids)
                    if (self.myclass == models.Work) and (run_method in ["store", "store_work_high", "store_work_low"]):
                        # no abstracts
                        objects = db.session.query(models.Work).options(
                             selectinload(models.Work.locations),
                             selectinload(models.Work.journal).selectinload(models.Venue.journalsdb),
                             selectinload(models.Work.references),
                             selectinload(models.Work.mesh),
                             selectinload(models.Work.counts_by_year),
                             # selectinload(models.Work.abstract),
                             selectinload(models.Work.extra_ids),
                             selectinload(models.Work.related_works),
                             selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids),
                             selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror),
                             selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept),
                             orm.Load(models.Work).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                    elif self.myclass == models.Work and run_method != "new_work_concepts":
                        objects = db.session.query(models.Work).options(
                             selectinload(models.Work.locations),
                             selectinload(models.Work.journal).selectinload(models.Venue.journalsdb),
                             selectinload(models.Work.references),
                             selectinload(models.Work.mesh),
                             selectinload(models.Work.counts_by_year),
                             selectinload(models.Work.abstract),
                             selectinload(models.Work.extra_ids),
                             selectinload(models.Work.related_works),
                             selectinload(models.Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids),
                             selectinload(models.Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror),
                             selectinload(models.Work.concepts).selectinload(models.WorkConcept.concept),
                             orm.Load(models.Work).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                    elif self.myclass == models.Work and run_method=="new_work_concepts":
                        # objects = db.session.query(models.Work).options(
                        #      selectinload(models.Work.journal).selectinload(models.Venue.journalsdb),
                        #      orm.Load(models.Work).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                        q = """select work.paper_id, work.paper_title, work.doc_type, journal.display_name as journal_title
                            from mid.work work
                            left outer join mid.journal journal on journal.journal_id=work.journal_id 
                            where paper_id in ({})
                        """.format(",".join(str(paper_id) for paper_id in object_ids))
                        try:
                            objects = db.session.execute(text(q)).fetchall()
                        except:
                            objects = []
                    elif self.myclass == models.Record:
                        objects = db.session.query(models.Record).options(
                             selectinload(models.Record.work_matches_by_title).raiseload('*'),
                             selectinload(models.Record.work_matches_by_doi).raiseload('*'),
                             orm.Load(models.Record).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                    elif self.myclass == models.Author:
                        objects = db.session.query(models.Author).options(
                             selectinload(models.Author.counts_by_year),
                             selectinload(models.Author.alternative_names),
                             selectinload(models.Author.author_concepts),
                             selectinload(models.Author.orcids).selectinload(models.AuthorOrcid.orcid_data),
                             selectinload(models.Author.last_known_institution).raiseload('*'),
                             orm.Load(models.Author).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                    elif self.myclass == models.Institution:
                        objects = db.session.query(models.Institution).options(
                             selectinload(models.Institution.counts_by_year),
                             selectinload(models.Institution.ror).raiseload('*'),
                             orm.Load(models.Institution).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                    elif self.myclass == models.Concept:
                        objects = db.session.query(models.Concept).options(
                             selectinload(models.Concept.counts_by_year),
                             selectinload(models.Concept.ancestors),
                             orm.Load(models.Concept).raiseload('*')).filter(self.myid.in_(object_ids)).all()
                    elif self.myclass == models.Venue:
                        objects = db.session.query(models.Venue).options(
                             selectinload(models.Venue.counts_by_year),
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

        print("done")
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