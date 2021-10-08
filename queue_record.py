import argparse
import logging
import os
import random
from time import sleep
from time import time
import shortuuid

from sqlalchemy import orm
from sqlalchemy import text

from app import db
from app import logger
from record import Record
from queue_main import DbQueue
from util import elapsed
from util import normalize_doi
from util import run_sql


class DbQueueRecord(DbQueue):
    def table_name(self, job_type):
        table_name = "recordthresher_record"
        return table_name

    def process_name(self, job_type):
        if self.parsed_vars:
            process_name = self.parsed_vars.get("method")
        return process_name

    def worker_run(self, **kwargs):
        single_obj_id = kwargs.get("id", None)
        chunk = kwargs.get("chunk", 100)
        limit = kwargs.get("limit", 10)
        run_class = Record
        run_method = kwargs.get("method")

        if single_obj_id:
            limit = 1
            queue_table = None
        else:
            queue_table = "recordthresher_record"
            if not limit:
                limit = 1000
            text_query_pattern = """
                begin transaction read write;
                update {queue_table} set started=sysdate, started_label='{started_label}'
                    where id in
                        (select id
                        FROM   {queue_table}
                        WHERE  started is null and finished is null
                        LIMIT  {chunk});
                commit;
                end;
                select id from {queue_table} where started_label='{started_label}';
            """

        index = 0
        start_time = time()
        while True:
            new_loop_start_time = time()
            started_label = shortuuid.uuid()[0:10]
            text_query = text_query_pattern.format(
                limit=limit,
                chunk=chunk,
                queue_table=queue_table,
                started_label=started_label
            )
            # logger.info("the queue query is:\n{}".format(text_query))

            if single_obj_id:
                single_obj_id = normalize_doi(single_obj_id)
                objects = [run_class.query.filter(run_class.id == single_obj_id).first()]
            else:
                logger.info("looking for new jobs")

                job_time = time()
                row_list = db.engine.execute(text(text_query)).fetchall()
                object_ids = [row[0] for row in row_list]
                logger.info("got ids, took {} seconds".format(elapsed(job_time)))

                job_time = time()
                q = db.session.query(Record).options(orm.undefer('*')).filter(Record.id.in_(object_ids))
                objects = q.all()
                logger.info("got record objects in {} seconds".format(elapsed(job_time)))

                # shuffle them or they sort by doi order
                random.shuffle(objects)

                # objects = Record.query.from_statement(text(text_query)).execution_options(autocommit=True).all()

                # objects = run_class.query.from_statement(text(text_query)).execution_options(autocommit=True).all()
                # id_rows =  db.engine.execute(text(text_query)).fetchall()
                # ids = [row[0] for row in id_rows]
                #
                # job_time = time()
                # objects = run_class.query.filter(run_class.id.in_(ids)).all()

                # logger.info(u"finished get-new-objects query in {} seconds".format(elapsed(job_time)))

            if not objects:
                # logger.info(u"sleeping for 5 seconds, then going again")
                sleep(5)
                continue

            object_ids = [obj.id for obj in objects]
            self.update_fn(run_class, run_method, objects, index=index)

            # logger.info(u"finished update_fn")
            if queue_table:
                object_ids_str = ",".join(["'{}'".format(id.replace("'", "''")) for id in object_ids])
                object_ids_str = object_ids_str.replace("%", "%%")  #sql escaping
                sql_command = "update {queue_table} set finished=sysdate, started=null where id in ({ids})".format(
                    queue_table=queue_table, ids=object_ids_str)
                logger.info(u"sql command to update finished is: {}".format(sql_command))
                run_sql(db, sql_command)
                # logger.info(u"finished run_sql")

            # finished is set in update_fn
            index += 1
            if single_obj_id:
                return
            else:
                self.print_update(new_loop_start_time, chunk, limit, start_time, index)


if __name__ == "__main__":
    if os.getenv('OADOI_LOG_SQL'):
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        db.session.configure()

    parser = argparse.ArgumentParser(description="Run stuff.")
    parser.add_argument('--id', nargs="?", type=str, help="id of the one thing you want to update (case sensitive)")
    parser.add_argument('--doi', nargs="?", type=str, help="id of the one thing you want to update (case insensitive)")
    parser.add_argument('--method', nargs="?", type=str, default="update", help="method name to run")

    parser.add_argument('--reset', default=False, action='store_true', help="do you want to just reset?")
    parser.add_argument('--run', default=False, action='store_true', help="to run the queue")
    parser.add_argument('--status', default=False, action='store_true', help="to logger.info(the status")
    parser.add_argument('--dynos', default=None, type=int, help="scale to this many dynos")
    parser.add_argument('--logs', default=False, action='store_true', help="logger.info(out logs")
    parser.add_argument('--monitor', default=False, action='store_true', help="monitor till done, then turn off dynos")
    parser.add_argument('--kick', default=False, action='store_true', help="put started but unfinished dois back to unstarted so they are retried")
    parser.add_argument('--limit', "-l", nargs="?", type=int, help="how many jobs to do")
    parser.add_argument('--chunk', "-ch", nargs="?", default=500, type=int, help="how many to take off db at once")

    parsed_args = parser.parse_args()

    job_type = "normal"  #should be an object attribute
    my_queue = DbQueueRecord()
    my_queue.parsed_vars = vars(parsed_args)
    my_queue.run_right_thing(parsed_args, job_type)
