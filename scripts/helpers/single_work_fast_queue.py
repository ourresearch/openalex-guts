# -*- coding: utf-8 -*-

DESCRIPTION = """run fast queue for one work and refresh elasticsearch index"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

from elasticsearch import Elasticsearch
from app import WORKS_INDEX, WORKS_INDEX_PREFIX, db, ELASTIC_URL
from scripts.fast_queue import get_objects, index_and_merge_object_records
from models.work import elastic_index_suffix


def store_obj(o):
    bulk_actions = []
    record_actions = o.store()
    bulk_actions += [bulk_action for bulk_action in record_actions if bulk_action]
    index_and_merge_object_records(bulk_actions)
    db.session.commit()


def refresh_index(o):
    es = Elasticsearch([ELASTIC_URL], timeout=30)
    index_suffix = elastic_index_suffix(o.year)
    index_name = f"{WORKS_INDEX_PREFIX}-{index_suffix}"
    es.indices.refresh(index=index_name)


def store_and_refresh_index(work_id):
    o = get_objects("work", [work_id])[0]
    store_obj(o)
    refresh_index(o)


def main(args):
    try:
        work_id = args.work_id
        store_and_refresh_index(work_id)
    finally:
        db.session.close()


if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info("{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
    logger.info("pid: {}".format(os.getpid()))
    import argparse

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("work_id", type=int, help="work ID (integer)")
    parser.add_argument("--debug", action="store_true", help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug("debug mode is on")
    main(args)
    total_end = timer()
    logger.info(
        "all finished. total time: {}".format(format_timespan(total_end - total_start))
    )
