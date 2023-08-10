# -*- coding: utf-8 -*-

DESCRIPTION = """For each journal, get count of works by work type"""

import sys, os, time, json, gzip
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

from cleanup.util import paginate_openalex, make_request

def main(args):
    # get all journal source IDs
    mailto = args.email
    outfp = Path(args.output)
    all_journals = []
    url = "https://api.openalex.org/sources"
    params = {
        'mailto': mailto,
        'filter': 'type:journal',
        'select': 'id',
    }
    for r in paginate_openalex(url, params):
        for result in r.json()['results']:
            all_journals.append(result['id'])

    # make one groupby query per journal
    logger.info(f"writing to {outfp}")
    with gzip.open(outfp, mode='wt') as outf:
        logger.info(f"collecting data for {len(all_journals)} source IDs...")
        data = []
        for i, source_id in enumerate(all_journals):
            url = f"https://api.openalex.org/works?filter=primary_location.source.id:{source_id}&group_by=type&mailto={mailto}"
            r = make_request(url)
            for item in r.json()['group_by']:
                this_line = {
                    'source_id': source_id,
                    'work_type': item['key'],
                    'n_works': item['count'],
                }
                outf.write(json.dumps(this_line) + "\n")
            if i in [10, 100, 1000, 5000, 10000] or i % 50000 == 0:
                logger.info(f"{i} processed")


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
    parser.add_argument("output", help="output file (.gz)")
    parser.add_argument("--email", default="dev@ourresearch.org", help="email address to use in the requests")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))