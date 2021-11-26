import argparse
import logging
import os

from app import db
from queues.queue_base import DbQueue


class DbQueueOpenAlex(DbQueue):

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
        return myclass

    @property
    def myid(self):
        import models

        table = self.parsed_vars.get("table")
        if table == "record":
            myid = models.Record.id
        elif table == "work":
            myid = models.Work.paper_id
        return myid

    @property
    def id_field_name(self):
        table = self.parsed_vars.get("table")
        if table == "work":
            return "paper_id"
        return "id"

    def process_name(self, job_type):
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

    my_queue = DbQueueOpenAlex()
    my_queue.parsed_vars = vars(parsed_args)
    job_type = "normal"  #should be an object attribute
    my_queue.run_right_thing(parsed_args, job_type)



# unload ($$ select '["' || paper_id || '"],' as line from mid.work_json $$)
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