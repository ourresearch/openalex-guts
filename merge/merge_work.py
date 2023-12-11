import argparse
import csv
import datetime

from app import db, logger


"""
1. Run this script to merge work(s).
  single work: heroku run python -- -m merge.merge_work --old_id=123 --merge_into_id=456
  or
  csv with header old_id, merge_into_id: heroku run python -- -m merge.merge_work --input_file=merge_works.csv 
"""


def parse_arguments():
    parser = argparse.ArgumentParser(description="Merge works script.")
    parser.add_argument(
        "-f",
        "--input_file",
        help="Input CSV file for merging works, use header old_id, merge_into_id",
        default=None,
    )
    parser.add_argument(
        "-o", "--old_id", help="Old work ID", type=int, default=None
    )
    parser.add_argument(
        "-m",
        "--merge_into_id",
        help="Work ID to merge into",
        type=int,
        default=None,
    )
    return parser.parse_args()


def merge_works(input_file, old_id, merge_into_id):
    if input_file:
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                count += 1
                logger.info(f"Processing row {count}")
                process_work(int(row["old_id"]), int(row["merge_into_id"]))
    elif old_id is not None and merge_into_id is not None:
        process_work(old_id, merge_into_id)
    else:
        raise ValueError(
            "Either an input file must be provided or both old_id and merge_into_id must be specified."
        )
    logger.info("Done.")


def process_work(old_id, merge_into_id):
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Merging {old_id} into {merge_into_id}")

    # update mid.work
    logger.info(
        f"Updating merge_into_id and merge_into_date for {old_id} into {merge_into_id}."
    )
    merge_work_sql = f"""
                    UPDATE mid.work
                    SET merge_into_id = {merge_into_id},
                        merge_into_date = '{current_datetime}',
                        updated_date = '{current_datetime}'
                    WHERE paper_id = {old_id}
                """
    response = db.engine.execute(merge_work_sql)
    logger.info(f"Rows affected: {response.rowcount}")

    # update ins.recordthresher_record
    logger.info(f"Updating recordthresher records for {old_id} into {merge_into_id}")
    recordthresher_sql = f"""
                    UPDATE ins.recordthresher_record
                    SET work_id = {merge_into_id},
                        updated = '{current_datetime}'
                    WHERE work_id = {old_id}
                """
    response = db.engine.execute(recordthresher_sql)
    logger.info(f"Rows affected: {response.rowcount}")


if __name__ == "__main__":
    args = parse_arguments()
    merge_works(args.input_file, args.old_id, args.merge_into_id)
