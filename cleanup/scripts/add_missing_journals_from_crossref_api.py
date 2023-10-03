# -*- coding: utf-8 -*-

DESCRIPTION = (
    """Get all journals from Crossref's API. Add new journals to mid.journal."""
)

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

from app import db
from cleanup.util import make_request, add_new_source_to_db
from sqlalchemy import text


def get_journals_from_crossref_api():
    journals = []
    params = {
        "rows": 1000,
        "mailto": 'dev@ourresearch.org',
    }
    cursor = "*"
    url = "https://api.crossref.org/journals"
    fields_to_save = [
        "last-status-check-time",
        "counts",
        "publisher",
        "title",
        "subjects",
        "ISSN",
        "issn-type",
    ]
    while True:
        params["cursor"] = cursor
        r = make_request(url, params=params)
        message = r.json()["message"]
        items = message["items"]
        if len(items) == 0:
            break
        for item in items:
            journals.append({field: item.get(field) for field in fields_to_save})
        cursor = message["next-cursor"]
    return journals


def get_openalex_sources():
    results = db.session.execute(
        text(
            "select journal_id, issns, issns_text_array, publisher_id, original_publisher from mid.journal"
        )
    ).all()
    return results


def main(args):
    num_added = 0
    logger.info("getting journals from crossref api...")
    crossref_journals = get_journals_from_crossref_api()
    logger.info(f"found {len(crossref_journals)} in crossref api")
    logger.info("getting openalex sources from mid.journal")
    openalex_sources = get_openalex_sources()
    logger.info(f"found {len(openalex_sources)} openalex sources")
    issns_in_openalex = set()
    for item in openalex_sources:
        if item.issns_text_array:
            for issn in item.issns_text_array:
                issns_in_openalex.add(issn)
    logger.info(f"there are {len(issns_in_openalex)} unique ISSNs in openalex")
    logger.info("adding new sources to openalex...")
    commit = not args.dry_run
    for item in crossref_journals:
        crossref_item_issns = item["ISSN"]
        if crossref_item_issns:
            if not any([issn in issns_in_openalex for issn in crossref_item_issns]):
                # none of the ISSNs from crossref can be found in OpenAlex, so add them
                logger.info(f"adding journal {item['title']} (ISSN: {item['ISSN']})")
                add_new_source_to_db(
                    issn_list=item["ISSN"],
                    display_name=item["title"],
                    source_type="journal",
                    publisher_str=item["publisher"],
                    session=db.session,
                    commit=commit,
                    check_for_existing=False,
                )
                num_added += 1
    logger.info(f"added {num_added} new sources to openalex")


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
    parser.add_argument("--dry-run", action="store_true", help="don't commit changes to database")
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
