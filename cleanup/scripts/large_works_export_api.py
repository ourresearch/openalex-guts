# -*- coding: utf-8 -*-

DESCRIPTION = """get a lot of works from the api and save to gzip json lines files"""

import sys, os, time, gzip, json
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

from cleanup.util import paginate_openalex


def main(args):
    max_file_size = 5 * 1024**3  # 5GB uncompressed
    count = 0
    outdir = Path(args.outdir)
    logger.info(f"creating output directory: {outdir}")
    outdir.mkdir()
    part_file_number = 0
    part_file = None  # Initialize as None
    total_size = 0

    mailto = args.mailto
    url = "https://api.openalex.org/works"
    params = {
        "mailto": mailto,
        "filter": "publication_year:2015-2022",
        "select": "id,doi,publication_year,publication_date,primary_location,type,type_crossref,authorships,open_access,concepts,referenced_works_count,updated_date,created_date",
    }
    for r in paginate_openalex(url, params=params):
        if part_file is None:
            outfp = outdir.joinpath(f"part_{str(part_file_number).zfill(3)}.gz")
            part_file = gzip.open(outfp, "wt")
        for record in r.json()["results"]:
            count += 1
            line = json.dumps(record) + "\n"
            line_size = len(line.encode("utf-8"))
            # If this line will make the file exceed the max size, close the current file and open a new one
            if total_size + line_size > max_file_size:
                part_file.close()
                part_file_number += 1
                outfp = outdir.joinpath(f"part_{str(part_file_number).zfill(3)}.gz")
                part_file = gzip.open(outfp, "wt")
                total_size = 0

            if count % 10000 == 0:
                logger.info(f"count is {count}")

            part_file.write(line)
            total_size += line_size
    if part_file is not None:  # If file was created, close it
        logger.info(f"closing final file: {outfp}")
        part_file.close()
    logger.info(f"all done. count is {count}")


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
    parser.add_argument("mailto", help="email address to identify self to OpenAlex API")
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
