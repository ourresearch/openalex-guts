import argparse
import datetime
import logging
import os
import subprocess
import time

import boto3
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Run with: heroku local:run python -- -m scripts.pg_to_redshift --entity institution
"""

postgres_db_url = os.getenv("POSTGRES_URL")
redshift_db_url = os.getenv("REDSHIFT_SERVERLESS_URL")
s3_bucket = 'redshift-openalex'

if not postgres_db_url or not redshift_db_url:
    raise EnvironmentError("Both POSTGRES_URL and REDSHIFT_URL environment variables must be set")

current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

s3_client = boto3.client('s3')

queries = {
    "affiliation": "SELECT paper_id, author_id, affiliation_id, author_sequence_number, original_author, original_orcid FROM mid.affiliation",
    "institution": "SELECT * FROM mid.institution",
    "work": "SELECT paper_id, original_title, doi_lower, journal_id, merge_into_id, publication_date, doc_type, genre, arxiv_id, is_paratext, best_url, best_free_url, created_date FROM mid.work",
    "work_concept": "SELECT paper_id, field_of_study FROM mid.work_concept WHERE score > 0.3",
}


def get_s3_key(entity):
    """generate S3 key for the given entity."""
    return f"{entity}s_{current_date}.csv"


def export_postgres_to_s3(query, s3_key, entity):
    """execute the query and copy the output to S3 via a CSV file."""
    command = f"""
    psql {postgres_db_url} -c "\\COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');" | aws s3 cp - s3://{s3_bucket}/{s3_key}
    """
    logger.info(f"Executing command to save {entity}s to s3://{s3_bucket}/{s3_key}")
    start_time = time.time()
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        logger.error(f"Error during copy to S3 for entity {entity}: {stderr.decode('utf-8')}")
        raise RuntimeError(f"Failed to copy data to S3. Command: {command}")
    else:
        logger.info(f"Successfully copied data to S3 for entity {entity}: {stdout.decode('utf-8')} in {time.time() - start_time:.2f} seconds")


def truncate_staging_table(redshift_engine, entity):
    """truncate the staging table to ensure it's empty before loading new data."""
    truncate_sql = f"TRUNCATE TABLE {entity}_staging;"
    logger.info(f"Truncating staging table {entity}_staging")
    with redshift_engine.connect() as connection:
        connection.execute(truncate_sql)


def copy_s3_to_redshift_staging(s3_key, redshift_engine, entity):
    """copy data from S3 to Redshift staging table."""
    copy_sql = f"""
        COPY {entity}_staging
        FROM 's3://{s3_bucket}/{s3_key}'
        IAM_ROLE default
        FORMAT AS CSV
        DELIMITER ','
        IGNOREHEADER 1;
    """
    logger.info(f"Copying data from s3://{s3_bucket}/{s3_key} to redshift")
    start_time = time.time()
    try:
        with redshift_engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(copy_sql)
            logger.info(f"Successfully copied data to redshift {entity}_staging in {time.time() - start_time:.2f} seconds")
    except Exception as e:
        logger.error(f"Failed to copy data to Redshift: {str(e)}")
        raise


def replace_existing_data(redshift_engine, entity):
    """replace existing data in Redshift with data from staging table in one transaction."""
    replace_sql = f"""
        BEGIN;
        TRUNCATE TABLE {entity};
        INSERT INTO {entity}
        SELECT * FROM {entity}_staging;
        END;
    """
    logger.info(f"Replacing existing data in {entity} with data from {entity}_staging")
    start_time = time.time()
    with redshift_engine.connect() as connection:
        connection.execute(replace_sql)
        logger.info(f"Successfully replaced data in {entity} in {time.time() - start_time:.2f} seconds")


def delete_s3_file(s3_key):
    """delete the left over S3 file."""
    logger.info(f"Deleting S3 file s3://{s3_bucket}/{s3_key}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    logger.info(f"Successfully deleted S3 file s3://{s3_bucket}/{s3_key}")


def main(entity):
    query = queries.get(entity)
    if not query:
        raise ValueError(f"Entity {entity} not found in queries")

    redshift_engine = create_engine(redshift_db_url)
    s3_key = get_s3_key(entity)

    start_time = time.time()
    export_postgres_to_s3(query, s3_key, entity)
    truncate_staging_table(redshift_engine, entity)
    copy_s3_to_redshift_staging(s3_key, redshift_engine, entity)
    replace_existing_data(redshift_engine, entity)
    delete_s3_file(s3_key)
    truncate_staging_table(redshift_engine, entity)
    logger.info(f"{entity} completed in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL script to copy data from PostgreSQL to Redshift via S3.")
    parser.add_argument("--entity", required=True, type=str,
                        help="The entity to process (e.g., affiliation, institution, work, work_concept).")
    args = parser.parse_args()

    entity = args.entity
    try:
        main(entity)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
