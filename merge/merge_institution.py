import argparse
import csv
import datetime

from app import db, logger


"""
1. Run this script to merge institution(s).
  single institution: heroku run python -- -m merge.merge_institution --old_id=123 --merge_into_id=456
  or
  csv with header old_id, merge_into_id: python -m merge.merge_institution --input_file=merge_institutions.csv 
2. Notify Justin so he can update AND.
3. You may need to run this again to update mid.affiliation, since AND may have assigned using old ids.
"""


def parse_arguments():
    parser = argparse.ArgumentParser(description="Merge institutions script.")
    parser.add_argument(
        "-f",
        "--input_file",
        help="Input CSV file for merging institutions, use header old_id, merge_into_id",
        default=None,
    )
    parser.add_argument(
        "-o", "--old_id", help="Old institution ID", type=int, default=None
    )
    parser.add_argument(
        "-m",
        "--merge_into_id",
        help="Institution ID to merge into",
        type=int,
        default=None,
    )
    return parser.parse_args()


def merge_institutions(input_file, old_id, merge_into_id):
    if input_file:
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                count += 1
                logger.info(f"Processing row {count}")
                process_institution(int(row["old_id"]), int(row["merge_into_id"]))
    elif old_id is not None and merge_into_id is not None:
        process_institution(old_id, merge_into_id)
    else:
        raise ValueError(
            "Either an input file must be provided or both old_id and merge_into_id must be specified."
        )
    logger.info("Done.")


def process_institution(old_id, merge_into_id):
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Merging {old_id} into {merge_into_id}")

    # update mid.institution
    logger.info(
        f"Updating merge_into_id and merge_into_date for {old_id} into {merge_into_id}. Set ror_id to null."
    )
    merge_institution_sql = f"""
                    UPDATE mid.institution
                    SET merge_into_id = {merge_into_id},
                        merge_into_date = '{current_datetime}',
                        ror_id = null
                    WHERE affiliation_id = {old_id}
                """
    response = db.engine.execute(merge_institution_sql)
    logger.info(f"Rows affected: {response.rowcount}")

    # update mid.affiliation
    logger.info(f"Updating affiliation table for {old_id} into {merge_into_id}")
    affiliation_sql = f"""
                    UPDATE mid.affiliation
                    SET affiliation_id = {merge_into_id}
                    WHERE affiliation_id = {old_id}
                """
    response = db.engine.execute(affiliation_sql)
    logger.info(f"Rows affected: {response.rowcount}")

    # update mid.affiliation_string_v2.affiliation_id, replacing the old id with the new id
    logger.info(f"Updating affiliation_string_v2 table for {old_id} into {merge_into_id}")
    affiliation_strings_sql = f"""
                    UPDATE mid.affiliation_string_v2
                    SET affiliation_ids = jsonb_set(
                        affiliation_ids,
                        array[(SELECT idx - 1 FROM jsonb_array_elements_text(affiliation_ids) WITH ORDINALITY arr(element, idx) WHERE element::bigint = {old_id})::text],
                        to_jsonb({merge_into_id}::bigint),
                        false
                    )
                    WHERE affiliation_ids @> to_jsonb({old_id}::bigint)
                """
    response = db.engine.execute(affiliation_strings_sql)
    logger.info(f"Rows affected: {response.rowcount}")

    # update mid.affiliation_string_v2.affiliation_ids_override, replacing the old id with the new id
    logger.info(
        f"Updating affiliation_string_v2 table (override) for {old_id} into {merge_into_id}"
    )
    affiliation_strings_override_sql = f"""
                    UPDATE mid.affiliation_string_v2
                    SET affiliation_ids_override = jsonb_set(
                        affiliation_ids_override,
                        array[(SELECT idx - 1 FROM jsonb_array_elements_text(affiliation_ids_override) WITH ORDINALITY arr(element, idx) WHERE element::bigint = {old_id})::text],
                        to_jsonb({merge_into_id}::bigint),
                        false
                    )
                    WHERE affiliation_ids @> to_jsonb({old_id}::bigint)
                """
    response = db.engine.execute(affiliation_strings_override_sql)
    logger.info(f"Rows affected: {response.rowcount}")


if __name__ == "__main__":
    args = parse_arguments()
    merge_institutions(args.input_file, args.old_id, args.merge_into_id)
