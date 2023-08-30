# -*- coding: utf-8 -*-

DESCRIPTION = (
    """check a set of OpenAlex work IDs and see if they have sources assigned"""
)

import sys, os, time
from simplejson.scanner import JSONDecodeError
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

from cleanup.util import openalex_entities_by_ids


def output_logline(has_source, num_processed, start_time, query_index):
    time_elapsed = timer() - start_time
    msg = f"query_index={query_index}. So far: processed {num_processed} in {format_timespan(time_elapsed)} ({num_processed / time_elapsed : .02f} items/s). {has_source} of them have source assigned ({has_source / num_processed : .0%})"
    logger.info(msg)


def main(args):
    fp = Path(args.input)
    work_ids = fp.read_text().split("\n")
    work_ids = [work_id for work_id in work_ids if work_id]
    outfp = Path(args.output)
    mailto = args.mailto
    logger.debug(f"opening file for write: {outfp}")
    with outfp.open("w") as outf:
        logger.info(
            f"preparing to validate {len(work_ids)} works--- checking to see if they have sources"
        )
        logger.info(
            f"any works that do not have sources will be written to file: {outfp}"
        )
        has_source = 0
        num_processed = 0
        params = {
            "select": "id,doi,primary_location,updated_date",
            "mailto": mailto,
        }
        start_of_queries = timer()
        query_index = 0
        for r in openalex_entities_by_ids(work_ids, params=params):
            query_index += 1
            try:
                this_results = r.json()["results"]
                for w in this_results:
                    this_work_has_source = False
                    if "primary_location" in w:
                        primary_location = w["primary_location"]
                        if primary_location["source"] is not None:
                            this_work_has_source = True
                    if this_work_has_source is True:
                        has_source += 1
                    else:
                        print(w["id"], file=outf)
                # primary_locations = [w['primary_location'] for w in this_results if 'primary_location' in w]
                # has_source += sum([loc['source'] is not None for loc in primary_locations])
                num_processed += len(this_results)
                if query_index % 500 == 0:
                    output_logline(has_source, num_processed, start_of_queries, query_index)
            except JSONDecodeError:
                logger.error(f"JSONDecodeError encountered for query with url: {r.url}")
                continue
    output_logline(has_source, num_processed, start_of_queries, query_index)
    logger.info(f"done. {query_index} queries made.")


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
    parser.add_argument("input", help="path to newline separated list of work IDs")
    parser.add_argument(
        "output",
        help="path to output file, which will have the IDs for works that failed the validation",
    )
    parser.add_argument("mailto", help="email address to use for OpenAlex API requests")
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
