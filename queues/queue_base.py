import datetime
import random
from time import sleep
from time import time
import shortuuid
from sqlalchemy import text
from sqlalchemy import orm

from app import db
from app import logger
from util import elapsed
from util import safe_commit


class DbQueue(object):

    def __init__(self, **kwargs):
        self.parsed_vars = {}
        super(DbQueue, self).__init__(**kwargs)


    def update_fn(self, cls, method_name, objects, index=1):

        # we are in a fork!  dispose of our engine.
        # will get a new one automatically
        # if is pooling, need to do .dispose() instead
        db.engine.dispose()

        start = time()
        num_obj_rows = len(objects)

        # logger.info(u"{pid} {repr}.{method_name}() got {num_obj_rows} objects in {elapsed} seconds".format(
        #     pid=os.getpid(),
        #     repr=cls.__name__,
        #     method_name=method_name,
        #     num_obj_rows=num_obj_rows,
        #     elapsed=elapsed(start)
        # ))

        for count, obj in enumerate(objects):
            start_time = time()

            if obj is None:
                return None

            method_to_run = getattr(obj, method_name)

            # logger.info(u"***")
            logger.info("*** #{count} starting {repr}.{method_name}() method".format(
                count=count + (num_obj_rows*index),
                repr=obj,
                method_name=method_name
            ))

            method_to_run()

            logger.info("finished {repr}.{method_name}(). took {elapsed} seconds".format(
                repr=obj,
                method_name=method_name,
                elapsed=elapsed(start_time, 4)
            ))

            # for handling the queues
            if not (method_name == "update" and obj.__class__.__name__ == "Pub"):
                obj.finished = datetime.datetime.utcnow().isoformat()

            db.session.merge(obj)


        start_time = time()
        commit_success = safe_commit(db)
        if not commit_success:
            logger.info("COMMIT fail")
        logger.info("commit took {} seconds".format(elapsed(start_time, 2)))
        db.session.remove()  # close connection nicely
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
            if not limit:
                limit = 1000
            # order by random()
            text_query_pattern_update = """
                update {queue_table} set started=sysdate, started_label='{started_label}'
                    where {id_field_name} in
                        (select {id_field_name}
                        FROM   {queue_table}
                        WHERE  started is null and finished is null
                        and ((doc_type is null) or (doc_type != 'Patent'))
                        order by paper_id desc
                        LIMIT  {chunk});
            """
            text_query_pattern_select = """
                select {id_field_name} from {queue_table} where started_label='{started_label}';
            """

        index = 0
        start_time = time()
        while True:
            new_loop_start_time = time()
            started_label = shortuuid.uuid()[0:10]
            text_query_update = text_query_pattern_update.format(
                limit=limit,
                chunk=chunk,
                queue_table=queue_table,
                started_label=started_label,
                id_field_name=self.id_field_name
            )
            text_query_select = text_query_pattern_select.format(
                limit=limit,
                chunk=chunk,
                queue_table=queue_table,
                started_label=started_label,
                id_field_name=self.id_field_name
            )
            # logger.info("the queues query is:\n{}".format(text_query))

            if single_obj_id:
                single_obj_id = normalize_doi(single_obj_id)
                objects = [run_class.query.filter(run_class.id == single_obj_id).first()]
            else:
                logger.info("{}: looking for new jobs".format(worker_name))

                job_time = time()
                db.session.execute(text(text_query_update))
                row_list = db.session.execute(text(text_query_select)).fetchall()
                # db.session.commit()

                object_ids = [row[0] for row in row_list]
                logger.info("{}: got ids, took {} seconds".format(worker_name, elapsed(job_time)))


                job_time = time()
                print(object_ids)
                # q = db.session.query(self.myclass).filter(self.myclass.id.in_(object_ids))
                # q = db.session.query(self.myclass).options(orm.undefer('*')).filter(self.myclass.id.in_(object_ids))

                # q = db.session.query(self.myclass).options(orm.noload('*')).filter(self.myid.in_(object_ids))
                q = db.session.query(self.myclass).filter(self.myid.in_(object_ids))

                objects = q.all()
                # db.session.commit()
                logger.info("{}: got objects in {} seconds".format(worker_name, elapsed(job_time)))

                # shuffle them or they sort by doi order
                random.shuffle(objects)

                # text_query = "select * from recordthresher_record limit 10; "
                # objects = self.myclass.query.from_statement(text(text_query)).execution_options(autocommit=True).all()


                # objects = run_class.query.from_statement(text(text_query)).execution_options(autocommit=True).all()
                # print(objects)
                # id_rows =  db.engine.execute(text(text_query)).fetchall()
                # ids = [row[0] for row in id_rows]
                #
                # job_time = time()
                # objects = run_class.query.filter(run_class.id.in_(ids)).all()

                # logger.info(u"{}: finished get-new-objects query in {} seconds".format(worker_name, elapsed(job_time)))


                if not objects:
                    # logger.info(u"{}: sleeping for 5 seconds, then going again".format(worker_name)
                    sleep(5)
                    continue

                object_ids = [obj.id for obj in objects]
                self.update_fn(run_class, run_method, objects, index=index)

                # logger.info(u"{}: finished update_fn".format(worker_name)
                if queue_table:
                    # object_ids_str = ",".join(["'{}'".format(id.replace("'", "''")) for id in object_ids])
                    # object_ids_str = object_ids_str.replace("%", "%%")  #sql escaping
                    object_ids_str = ",".join(["'{}'".format(id) for id in object_ids])
                    sql_command = "update {queue_table} set finished=sysdate, started=null where id in ({ids})".format(
                        queue_table=queue_table, ids=object_ids_str)
                    # logger.info(u"{}: sql command to update finished is: {}".format(worker_name, sql_command))
                    # run_sql(db, sql_command)

                    db.session.execute(text(sql_command))

                    # logger.info(u"{}: finished run_sql".format(worker_name)

                index += 1
                if single_obj_id:
                    return
                else:
                    self.print_update(new_loop_start_time, chunk, limit, start_time, index)


    def run(self, parsed_args, job_type):
        start = time()

        try:
            self.worker_run(**vars(parsed_args))
        except Exception as e:
            logger.fatal('worker_run died with error:', exc_info=True)


        logger.info("finished update in {} seconds".format(elapsed(start)))
        # resp = None
        # if job_type in ["normal"]:
        #     my_location = Page.query.get(parsed_args.id)
        #     resp = my_location.__dict__
        #     pprint(resp)

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


    def run_right_thing(self, parsed_args, job_type):
        if parsed_args.id or parsed_args.doi or parsed_args.run:
            if parsed_args.randstart:
                sleep_time = round(random.random(), 2) * 10
                print("Sleeping to randomize start for {} seconds".format(sleep_time))
                sleep(sleep_time)
            self.run(parsed_args, job_type)


    ## these are overwritten by main class

    def process_name(self, job_type):
        pass

    def myclass(self):
        pass

    def table_name(self):
        pass
