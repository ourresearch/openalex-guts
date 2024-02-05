# -*- coding: utf-8 -*-

DESCRIPTION = """add the most recent crossref article to track"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer

import requests

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

from app import db
from tracking.models import RecordTrack


def query_crossref_api():
    params = {
        "sort": "created",
        "order": "desc",
        "rows": "1",
        "select": "DOI,publisher,created",
        "filter": "type:journal-article",
        "mailto": "team@ourresearch.org",
    }
    url = "https://api.crossref.org/works"
    r = requests.get(url, params=params)
    return r.json()["message"]["items"][0]


def main(args):
    item = query_crossref_api()
    doi = item["DOI"]
    if db.session.query(RecordTrack).filter_by(doi=doi).one_or_none():
        logger.info(f"doi {doi} is already being tracked, so not adding")
    else:
        rec = RecordTrack(doi=doi)
        rec.created_at = datetime.now()
        rec.origin = "crossref"
        rec.origin_timestamp = datetime.fromtimestamp(
            item["created"]["timestamp"] / 1000
        )
        rec.note = f'publisher: {item.get("publisher")}'
        logger.info(f"adding new RecordTrack with doi {doi}")
        db.session.add(rec)
        db.session.commit()
        logger.info(f'tracking record {rec.id}')
        rec.track()


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
