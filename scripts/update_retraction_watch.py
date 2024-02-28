# -*- coding: utf-8 -*-

DESCRIPTION = """update retraction watch data (https://doi.org/10.13003/c23rw1d9)"""

import sys, os, time
import io
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

import requests
import pandas as pd

from app import db
from scripts.front_of_fast_queue import front_of_fast_queue_one_batch


def request_retraction_watch_data():
    csv_download_url = "https://api.labs.crossref.org/data/retractionwatch"
    params = {
        "mailto": "dev@ourresearch.org",
    }
    logger.info(
        f"downloading retraction watch data from {csv_download_url} (using mailto: {params['mailto']})"
    )
    r = requests.get(csv_download_url, params=params)
    r.raise_for_status()
    return r


def main(args):
    r = request_retraction_watch_data()
    df = pd.read_csv(io.BytesIO(r.content), encoding_errors="replace")
    df["OriginalPaperDOILower"] = df["OriginalPaperDOI"].str.lower()
    df.rename(columns={"Record ID": "record_id"}, inplace=True)
    df.set_index("record_id", inplace=True)

    # store in database table
    tablename = "retraction_watch"
    schema = "ins"
    logger.info(f"replacing data in table: {schema}.{tablename}")
    df.to_sql(
        tablename,
        con=db.session.connection(),
        schema=schema,
        index=True,
        method="multi",
        if_exists="replace",
    )

    logger.info("collecting works to update from database...")
    sq = """
    select w.paper_id
    from mid.work w
    inner join ins.retraction_watch rw
        on w.doi_lower = rw."OriginalPaperDOILower"
    """
    work_ids = db.session.execute(sq).scalars().all()
    batch_size = 10000
    logger.info(
        f"moving {len(work_ids)} to the front of the fast queue for update (batch_size: {batch_size})"
    )
    work_ids_batches = [
        work_ids[i : i + batch_size] for i in range(0, len(work_ids), batch_size)
    ]
    for b in work_ids_batches:
        front_of_fast_queue_one_batch(b)


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
