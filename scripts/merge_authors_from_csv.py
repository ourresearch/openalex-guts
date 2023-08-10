# -*- coding: utf-8 -*-

DESCRIPTION = """given a csv file with 2 columns [merge_away_id, merge_into_id]
merge all of them, and move them to the front of the queue"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
import csv
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import logging
root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

from .merge import run as run_merge
from app import db
from sqlalchemy import text

def move_to_front_of_queue(author_id):
    q = """
    update queue.work_store qws
	set finished = null
    from mid.affiliation a
    where a.author_id = :author_id
    and a.paper_id = qws.id;
    """
    db.session.execute(text(q).bindparams(author_id=author_id))
    db.session.commit()

def main(args):
    with open(args.input, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        num_complete = 0
        for row in reader:
            merge_away_id = row['merge_away_id']
            merge_into_id = row['merge_into_id']
            logger.info(f"starting merge. merge_away_id: {merge_away_id} merge_into_id: {merge_into_id}")
            run_merge('author', merge_away_id, merge_into_id)
            move_to_front_of_queue(merge_into_id)
            num_complete += 1
    logger.info(f"finished processing {num_complete} authors")

if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s", datefmt="%H:%M:%S"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    logger.info("pid: {}".format(os.getpid()))
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("input", help="path to input file (csv)")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))