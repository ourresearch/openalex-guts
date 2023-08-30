# -*- coding: utf-8 -*-

DESCRIPTION = """update ror tables in the ins schema"""

import sys, os, time
import json
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
from cleanup.ror_models import (
    RorAcronyms,
    RorAddresses,
    RorAliases,
    RorBase,
    RorExternalIds,
    RorGridEquivalents,
    RorInstitutes,
    RorLabels,
    RorLinks,
    RorRelationships,
    RorTypes,
)


def ror_short_id(ror_id):
    return ror_id.replace("https://ror.org/", "")


def get_database_rows(cls, ror_id):
    return db.session.query(cls).filter_by(ror_id=ror_id).all()


def update_association_table(cls, item):
    ror_id = ror_short_id(item["id"])
    db_rows = get_database_rows(cls, ror_id)
    for obj in db_rows:
        db.session.delete(obj)
    for new_obj in cls.yield_from_ror_entry(item):
        db.session.add(new_obj)


def process_one_org(item):
    ror_id = ror_short_id(item["id"])
    if item["acronyms"]:
        update_association_table(RorAcronyms, item)
    if item["addresses"]:
        update_association_table(RorAddresses, item)
    if item["aliases"]:
        update_association_table(RorAliases, item)
    this_ror_base = get_database_rows(RorBase, ror_id)
    for obj in this_ror_base:
        db.session.delete(obj)
    new_obj = RorBase.from_ror_entry(item)
    db.session.add(new_obj)
    if item["external_ids"]:
        update_association_table(RorExternalIds, item)
        if "GRID" in item["external_ids"]:
            update_association_table(RorGridEquivalents, item)
    this_ror_institute = get_database_rows(RorInstitutes, ror_id)
    for obj in this_ror_institute:
        db.session.delete(obj)
    new_obj = RorInstitutes.from_ror_entry(item)
    db.session.add(new_obj)
    if item["labels"]:
        update_association_table(RorLabels, item)
    if item["links"]:
        update_association_table(RorLinks, item)
    if item["relationships"]:
        update_association_table(RorRelationships, item)
    if item["types"]:
        update_association_table(RorTypes, item)

    db.session.commit()


def main(args):
    fp = Path(args.input)
    ror_data = json.loads(fp.read_text())
    logger.info(f"beginning to process {len(ror_data)} ror records")
    for item in ror_data:
        logger.debug(f"processing ror id: {item['id']}")
        process_one_org(item)


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
    parser.add_argument("input", help="input filename (.json)")
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
