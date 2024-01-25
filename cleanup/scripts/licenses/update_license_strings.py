# -*- coding: utf-8 -*-

# THIS IS LEGACY CODE. PROBABLY NO LONGER THE BEST WAY TO DO THIS

DESCRIPTION = """update license strings"""

# example usage-- rename "pd" to "public-domain":
# python -m cleanup.scripts.licenses.update_license_strings pd public-domain

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

from app import db
from sqlalchemy import text

import logging
root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

def main(args):
    old_license_str = args.old_license_str
    new_license_str = args.new_license_str
    chunksize = 10000

    logger.info(f'getting ids from ins.recordthresher table')
    start = timer()
    stmt = "select id from ins.recordthresher_record where open_license like :old_license_str"
    ids = db.session.execute(text(stmt), {'old_license_str': old_license_str}).all()
    logger.info(f'found {len(ids)} rows in {format_timespan(timer()-start)}')
    logger.info(f'starting updates to ins.recordthresher_record with chunksize: {chunksize}')
    start = timer()
    for i in range(0, len(ids), chunksize):
        this_chunk = [row.id for row in ids[i:i+chunksize]]
        if this_chunk:
            logger.info(f'updating chunk starting with i={i}')
            stmt = "update ins.recordthresher_record set open_license = :new_license_str where id = ANY(:ids)"
            db.session.execute(text(stmt), {'new_license_str': new_license_str, 'ids': this_chunk})
            db.session.commit()
    logger.info(f'finished updating ins.recordthrehser_record in {format_timespan(timer()-start)}')

    logger.info(f'getting ids from mid.location table')
    start = timer()
    stmt = "select paper_id from mid.location where license like :old_license_str"
    ids = db.session.execute(text(stmt), {'old_license_str': old_license_str}).all()
    logger.info(f'found {len(ids)} rows in {format_timespan(timer()-start)}')
    logger.info(f'starting updates to mid.location with chunksize: {chunksize}')
    start = timer()
    for i in range(0, len(ids), chunksize):
        this_chunk = [row.paper_id for row in ids[i:i+chunksize]]
        if this_chunk:
            logger.info(f'updating chunk starting with i={i}')
            stmt = "update mid.location set license = :new_license_str where paper_id = ANY(:ids)"
            db.session.execute(text(stmt), {'new_license_str': new_license_str, 'ids': this_chunk})
            db.session.commit()
    logger.info(f'finished updating mid.location in {format_timespan(timer()-start)}')

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
    parser.add_argument("old_license_str", help="old license string (to rename)")
    parser.add_argument("new_license_str", help="new license string")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))