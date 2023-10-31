# -*- coding: utf-8 -*-

DESCRIPTION = """TODO: description"""

import sys, os, time, json, gzip, re
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
from elasticsearch_dsl import Search

from app import ELASTIC_URL

# Configure Elasticsearch client
es = Elasticsearch([ELASTIC_URL], timeout=60, max_retries=10, retry_on_timeout=True)

from cleanup.util import paginate_es


def check_existing(dirpath):
    file_numbers = []
    ignore_ids = set()
    for fp in dirpath.glob("affiliations_export_*.gz"):
        logger.info(f"checking existing data in {fp}")
        file_number = re.search(r"affiliations_export_(\d+?)", fp.name).group(1)
        file_numbers.append(int(file_number))
        with gzip.open(fp, "rt") as f:
            for line in f:
                if line:
                    w = json.loads(line)
                    ignore_ids.add((int(w["id"].replace("https://openalex.org/W", ""))))
        logger.info(f"{len(ignore_ids)} existing IDs seen so far")
    part_file_number = max(file_numbers) + 1 if len(file_numbers) > 0 else 0
    return ignore_ids, part_file_number


def main(args):
    outdir = Path(args.outdir)
    if not outdir.exists():
        outdir.mkdir()
    MAX_FILE_SIZE = 5 * 1024**3  # 5GB uncompressed
    PAGE_SIZE = 10000  # Set a page size for the search after queries
    count = 0
    index_id_prefix = f"https://openalex.org/W"

    if args.check_existing:
        ignore_ids, part_file_number = check_existing(outdir)
    else:
        ignore_ids = set()
        part_file_number = 0
    logger.info(f"starting data export. will ignore {len(ignore_ids)} IDs")
    outfp = outdir.joinpath(f"affiliations_export_{part_file_number:04d}.gz")
    logger.info(f"writing to output file {outfp}")
    part_file = gzip.open(str(outfp), "wt")
    total_size = 0

    s = Search(using=es, index="works-v19-*,-*invalid-data")
    s = s.sort(*["id"])
    s = s.source(
        excludes=[
            "_source",
            "fulltext",
            "abstract",
            "version",
            "@version",
            "@timestamp",
        ]
    )
    s = s.source(
        includes=["id", "authorships.author.id", "authorships.institutions.id"]
    )
    num_skipped = 0
    try:
        for hit in paginate_es(s, page_size=PAGE_SIZE):
            record_id = hit.id
            # convert to integer
            try:
                record_id = int(record_id.replace(index_id_prefix, ""))
            except ValueError:
                logger.warning(f"Skipping record {record_id}. Not an integer.")
                num_skipped += 1
                continue
            if not hit.id.startswith("https://openalex.org"):
                logger.warning(f"Skipping record {hit.id}")
                num_skipped += 1
                continue
            if ignore_ids and record_id in ignore_ids:
                num_skipped += 1
                if (
                    num_skipped in [100, 1000, 10000, 100000, 500000]
                    or num_skipped % 1000000 == 0
                ):
                    logger.info(f"num_skipped so far: {num_skipped}")
                continue
            count += 1
            line = json.dumps(hit.to_dict()) + "\n"
            line_size = len(line.encode("utf-8"))

            # If this line will make the file exceed the max size, close the current file and open a new one
            if total_size + line_size > MAX_FILE_SIZE:
                part_file.close()
                part_file_number += 1
                outfp = outdir.joinpath(
                    f"affiliations_export_{part_file_number:04d}.gz"
                )
                logger.info(f"writing to output file {outfp}")
                part_file = gzip.open(str(outfp), "wt")
                total_size = 0

            if count % 10000 == 0:
                logger.info(f"count is {count}")

            part_file.write(line)
            total_size += line_size
    finally:
        part_file.close()


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
    parser.add_argument("outdir", help="output directory")
    parser.add_argument(
        "--check-existing",
        action="store_true",
        help="look for existing data in the output directory, and skip those files",
    )
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
