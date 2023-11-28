# -*- coding: utf-8 -*-

DESCRIPTION = """Update institutions from ROR datadump"""

# Step 1: run this script (`python -m scripts.update_ror_institutions`)
# (this will take 8 hours or so)
#
# Step 2: upsert into ins.ror_summary table:
# ```sql
# insert into ins.ror_summary
# select * from ins.ror_summary_view
# on conflict (ror_id) do
# update set
# 	"name"=excluded."name",
# 	official_page=excluded.official_page,
# 	wikipedia_url=excluded.wikipedia_url,
# 	grid_id=excluded.grid_id,
# 	latitude=excluded.latitude,
# 	longitude=excluded.longitude,
# 	city=excluded.city,
# 	state=excluded.state,
# 	country=excluded.country,
# 	country_code=excluded.country_code,
# 	ror_type=excluded.ror_type
# ;
# ```
#
# Step 3: update existing rows in mid.institution
# ```sql
# update mid.institution i set
# 	display_name = ror."name",
# 	grid_id = coalesce (ror.grid_id, i.grid_id ),
# 	official_page = ror.official_page,
# 	wiki_page = ror.wikipedia_url,
# 	iso3166_code = ror.country_code,
# 	latitude = ror.latitude,
# 	longitude = ror.longitude,
# 	city = ror.city,
#   region = ror.state,
# 	country = ror.country,
# 	updated_date = now() at time zone 'utc'
# from ins.ror_summary ror
# where i.ror_id = ror.ror_id;
# ```
#
# Step 4: insert new rows in mid.institution
# ```sql
# with insert_rows as (
# select rs.*, i.affiliation_id
# from ins.ror_summary rs
# left join mid.institution i
# 	on rs.ror_id = i.ror_id
# where i.affiliation_id is null
# )
# insert into mid.institution (display_name, official_page, wiki_page, iso3166_code, latitude, longitude, grid_id, ror_id, city, region, country, created_date, updated_date)
# select name, official_page, wikipedia_url, country_code, latitude, longitude, grid_id, ror_id, city, state, country, now() at time zone 'utc', now() at time zone 'utc'
# from insert_rows;
# ```

import sys, os, time
import requests
import json
import io
from zipfile import ZipFile
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
from models.ror import (
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
    RorUpdates,
)
from sqlalchemy import text


def get_most_recent_ror_dump_metadata():
    # https://ror.readme.io/docs/data-dump#download-ror-data-dumps-programmatically-with-the-zenodo-api
    url = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    r = requests.get(url)
    if r.status_code >= 400:
        return None
    most_recent_hit = r.json()["hits"]["hits"][0]
    files = most_recent_hit["files"]
    most_recent_file_obj = files[-1]
    return most_recent_file_obj


def download_and_unzip_ror_data(url):
    r_zipfile = requests.get(url)
    r_zipfile.raise_for_status()
    with ZipFile(io.BytesIO(r_zipfile.content)) as myzip:
        for fname in myzip.namelist():
            if "ror-data" in fname and fname.endswith(".json"):
                with myzip.open(fname) as myfile:
                    ror_data = json.loads(myfile.read())
                    return ror_data
    return None


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


def refresh_ancestors_mv():
    sq = """refresh materialized view concurrently mid.institution_ancestors_mv"""
    db.engine.execute(text(sq))


def main(args):
    most_recent_file_obj = get_most_recent_ror_dump_metadata()
    if most_recent_file_obj is None:
        logger.info("Failed to get ROR data. Exiting without doing any updates...")
        return
    md5_checksum = most_recent_file_obj.get("checksum", "").replace("md5:", "")
    logger.info(f"most recent md5_checksum for ROR data: {md5_checksum}")
    most_recent_openalex_ror_update = (
        db.session.query(RorUpdates)
        .order_by(RorUpdates.finished_update_at.desc())
        .first()
    )
    if most_recent_openalex_ror_update:
        logger.info(
            f"The most recent ROR update in OpenAlex was: {most_recent_openalex_ror_update.finished_update_at or 'DID NOT COMPLETE'}"
        )
        logger.info(
            f"The most recent ROR update in OpenAlex had md5_checksum: {most_recent_openalex_ror_update.md5_checksum}"
        )
        if (
            most_recent_openalex_ror_update.md5_checksum == md5_checksum
            and most_recent_openalex_ror_update.finished_update_at is not None
        ):
            logger.info(
                f"md5_checksum matches most recent OpenAlex update. This means that ROR data is up to date in OpenAlex. Exiting without doing any updates... (md5_checksum: {md5_checksum})"
            )
            return
    logger.info("Beginning ROR update for OpenAlex")
    try:
        file_url = most_recent_file_obj["links"]["self"]
    except KeyError:
        logger.error("failed to update ROR data! Exiting without doing any updates...")
        raise
    filename = most_recent_file_obj.get("key")
    size = most_recent_file_obj.get("size")
    ror_update_log_db = RorUpdates(
        md5_checksum=md5_checksum, url=file_url, filename=filename, size=size
    )
    try:
        logger.info(f"downloading and unzipping ROR data from {file_url}")
        ror_data = download_and_unzip_ror_data(file_url)
        if not ror_data:
            raise RuntimeError(
                "failed to update ROR data! error encountered when trying to download and unzip ROR data! Exiting without doing any updates..."
            )
        logger.info(f"ROR data downloaded. there are {len(ror_data)} organizations")
        ror_update_log_db.downloaded_at = datetime.utcnow().isoformat()
        logger.info(f"beginning to process {len(ror_data)} ROR records")
        num_processed = 0
        for item in ror_data:
            if item.get('status') == 'withdrawn':
                # skip this one
                continue
            logger.debug(f"processing ROR ID: {item['id']}")
            process_one_org(item)
            num_processed += 1
            if num_processed in [10, 50, 100, 500, 1000, 5000, 10000] or num_processed % 20000 == 0:
                logger.info(f"{num_processed} processed so far")
        ror_update_log_db.finished_update_at = datetime.utcnow().isoformat()
    finally:
        logger.info("refreshing materialized view mid.institution_ancestors_mv")
        refresh_ancestors_mv()
        db.session.add(ror_update_log_db)
        db.session.commit()


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
