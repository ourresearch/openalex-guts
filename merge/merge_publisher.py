import argparse
import csv
import datetime

from app import db, logger


"""
1. Run this script to merge publisher(s).
  single publisher: heroku run python -- -m merge.merge_publisher --old_id=123 --merge_into_id=456
  or
  csv with header old_id, merge_into_id: heroku run python -- -m merge.merge_publisher --input_file=merge_publishers.csv 
"""


def parse_arguments():
    parser = argparse.ArgumentParser(description="Merge publishers script.")
    parser.add_argument(
        "-f",
        "--input_file",
        help="Input CSV file for merging publishers, use header old_id, merge_into_id",
        default=None,
    )
    parser.add_argument(
        "-o", "--old_id", help="Old publisher ID", type=int, default=None
    )
    parser.add_argument(
        "-m",
        "--merge_into_id",
        help="Publisher ID to merge into",
        type=int,
        default=None,
    )
    return parser.parse_args()


def merge_publishers(input_file, old_id, merge_into_id):
    if input_file:
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                count += 1
                logger.info(f"Processing row {count}")
                process_publisher(int(row["old_id"]), int(row["merge_into_id"]))
    elif old_id is not None and merge_into_id is not None:
        process_publisher(old_id, merge_into_id)
    else:
        raise ValueError(
            "Either an input file must be provided or both old_id and merge_into_id must be specified."
        )
    logger.info("Done.")


def process_publisher(old_id, merge_into_id):
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Merging {old_id} into {merge_into_id}")

    # update mid.publisher
    logger.info(
        f"Updating merge_into_id and merge_into_date for {old_id} into {merge_into_id}."
    )
    merge_publisher_sql = f"""
                    UPDATE mid.publisher
                    SET merge_into_id = {merge_into_id},
                        merge_into_date = '{current_datetime}'
                    WHERE publisher_id = {old_id}
                """
    response = db.engine.execute(merge_publisher_sql)
    logger.info(f"Rows affected: {response.rowcount}")

    # update mid.journal
    logger.info(f"Updating journal records for {old_id} into {merge_into_id}")
    journal_sql = f"""
                    UPDATE mid.journal
                    SET publisher_id = {merge_into_id},
                        updated_date = '{current_datetime}'
                    WHERE publisher_id = {old_id}
                """
    response = db.engine.execute(journal_sql)
    logger.info(f"Rows affected: {response.rowcount}")


if __name__ == "__main__":
    args = parse_arguments()
    merge_publishers(args.input_file, args.old_id, args.merge_into_id)
