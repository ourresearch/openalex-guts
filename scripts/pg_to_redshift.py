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
To add fields:
1. Add the field to the schemas dictionary under the appropriate table, setting the field name in the database and the datatype.
2. Run the update, for example if it is in the author table: heroku run:detached python -- -m scripts.pg_to_redshift --entity=author
3. View logs using the suggested log output to ensure the update was successful.
4. The update will be picked up on the next scheduled run if the entity is in advanced scheduler.
"""

postgres_db_url = os.getenv("POSTGRES_URL")
redshift_db_url = os.getenv("REDSHIFT_SERVERLESS_URL")
s3_bucket = 'redshift-openalex'
s3_client = boto3.client('s3')

if not postgres_db_url or not redshift_db_url:
    raise EnvironmentError("Both POSTGRES_URL and REDSHIFT_URL environment variables must be set")

redshift_engine = create_engine(redshift_db_url)
current_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

schemas = {
    "abstract": [
        ("paper_id", "BIGINT"),
        ("abstract", "VARCHAR(65535)")
    ],
    "affiliation": [
        ("paper_id", "BIGINT"),
        ("author_id", "BIGINT"),
        ("affiliation_id", "BIGINT"),
        ("author_sequence_number", "INTEGER"),
        ("original_author", "VARCHAR(65535)"),
        ("original_orcid", "VARCHAR(500)")
    ],
    "affiliation_string_v2": [
        ("original_affiliation", "VARCHAR(65535)"),
        ("affiliation_ids", "VARCHAR(65535)"),
        ("affiliation_ids_override", "VARCHAR(65535)"),
        ("updated", "TIMESTAMP"),
    ],
    "affiliation_unique_rows_view": [
        ("paper_id", "BIGINT"),
        ("author_id", "BIGINT"),
        ("affiliation_id", "BIGINT"),
    ],
    "author": [
        ("author_id", "BIGINT"),
        ("display_name", "VARCHAR(65535)"),
        ("merge_into_id", "BIGINT")
    ],
    "author_orcid": [
        ("author_id", "BIGINT"),
        ("orcid", "VARCHAR(500)")
    ],
    "citation": [
        ("paper_id", "BIGINT"),
        ("paper_reference_id", "BIGINT")
    ],
    "citation_institutions_mv": [
        ("affiliation_id", "BIGINT"),
        ("paper_count", "INTEGER"),
        ("oa_paper_count", "INTEGER"),
        ("citation_count", "INTEGER"),
    ],
    "citation_papers_mv": [
        ("paper_id", "BIGINT"),
        ("citation_count", "INTEGER")
    ],
    "country": [
        ("country_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
        ("continent_id", "INTEGER"),
        ("is_global_south", "BOOLEAN"),
    ],
    "continent": [
        ("continent_id", "INTEGER"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
        ("wikidata_id", "VARCHAR(500)"),
    ],
    "domain": [
        ("domain_id", "INTEGER"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
    ],
    "field": [
        ("field_id", "INTEGER"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
    ],
    "funder": [
        ("funder_id", "BIGINT"),
        ("display_name", "VARCHAR(65535)"),
        ("crossref_id", "BIGINT"),
        ("country_code", "VARCHAR(500)"),
        ("doi", "VARCHAR(500)"),
        ("description", "VARCHAR(65535)"),
        ("ror_id", "VARCHAR(500)"),
    ],
    "institution": [
        ("affiliation_id", "BIGINT"),
        ("display_name", "VARCHAR(65535)"),
        ("ror_id", "VARCHAR(500)"),
        ("iso3166_code", "VARCHAR(500)"),
    ],
    "institution_type": [
        ("institution_type_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
    ],
    "keyword": [
        ("keyword_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
    ],
    "language": [
        ("language_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
    ],
    "license": [
        ("license_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
    ],
    "publisher": [
        ("publisher_id", "BIGINT"),
        ("display_name", "VARCHAR(65535)"),
        ("country_code", "VARCHAR(500)"),
    ],
    "ror": [
        ("ror_id", "VARCHAR(500)"),
        ("name", "VARCHAR(65535)"),
        ("city", "VARCHAR(65535)"),
        ("state", "VARCHAR(65535)"),
        ("country", "VARCHAR(65535)"),
        ("country_code", "VARCHAR(500)"),
        ("grid_id", "VARCHAR(500)"),
        ("wikipedia_url", "VARCHAR(65535)"),
        ("ror_type", "VARCHAR(500)"),
    ],
    "sdg": [
        ("sdg_id", "INTEGER"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
    ],
    "source": [
        ("source_id", "BIGINT"),
        ("display_name", "VARCHAR(65535)"),
        ("type", "VARCHAR(500)"),
        ("issn", "VARCHAR(500)"),
        ("is_in_doaj", "BOOLEAN"),
        ("journal_id", "BIGINT"),
        ("publisher_id", "BIGINT"),
        ("apc_usd", "INTEGER"),
    ],
    "source_type": [
        ("source_type_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
    ],
    "subfield": [
        ("subfield_id", "INTEGER"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
    ],
    "topic": [
        ("topic_id", "INTEGER"),
        ("display_name", "VARCHAR(65535)"),
        ("summary", "VARCHAR(65535)"),
        ("keywords", "VARCHAR(65535)"),
        ("subfield_id", "INTEGER"),
        ("field_id", "INTEGER"),
        ("domain_id", "INTEGER"),
        ("wikipedia_url", "VARCHAR(65535)"),
    ],
    "work": [
        ("paper_id", "BIGINT"),
        ("original_title", "VARCHAR(65535)"),
        ("doi", "VARCHAR(500)"),
        ("doi_lower", "VARCHAR(500)"),
        ("journal_id", "BIGINT"),
        ("merge_into_id", "BIGINT"),
        ("publication_date", "VARCHAR(500)"),
        ("doc_type", "VARCHAR(500)"),
        ("genre", "VARCHAR(500)"),
        ("arxiv_id", "VARCHAR(500)"),
        ("is_paratext", "BOOLEAN"),
        ("best_url", "VARCHAR(65535)"),
        ("best_free_url", "VARCHAR(65535)"),
        ("oa_status", "VARCHAR(500)"),
        ("type", "VARCHAR(500)"),
        ("type_crossref", "VARCHAR(500)"),
        ("year", "INTEGER"),
        ("created_date", "VARCHAR(500)"),
        ("language", "VARCHAR(300)"),
        ("is_retracted", "BOOLEAN"),
        ("license", "VARCHAR(500)"),
    ],
    "work_concept": [
        ("paper_id", "BIGINT"),
        ("field_of_study", "BIGINT")
    ],
    "work_funder": [
        ("paper_id", "BIGINT"),
        ("funder_id", "BIGINT")
    ],
    "work_keyword": [
        ("paper_id", "BIGINT"),
        ("keyword_id", "VARCHAR(500)"),
        ("score", "FLOAT")
    ],
    "work_sdg": [
        ("paper_id", "BIGINT"),
        ("sdg_id", "INTEGER"),
        ("score", "FLOAT"),
    ],
    "work_topic": [
        ("paper_id", "BIGINT"),
        ("topic_id", "INTEGER"),
        ("score", "FLOAT"),
        ("topic_rank", "INTEGER")
    ],
    "work_type": [
        ("work_type_id", "VARCHAR(500)"),
        ("display_name", "VARCHAR(65535)"),
        ("description", "VARCHAR(65535)"),
    ],
    "work_fwci": [
        ("paper_id", "BIGINT"),
        ("publication_year", "INTEGER"),
        ("subfield_id", "INTEGER"),
        ("work_type", "VARCHAR(500)"),
        ("pub_plus_3_citations", "BIGINT"),
        ("fwci", "FLOAT"),
        ("update_date", "VARCHAR(500)"),
    ],
}


def get_schema_sql(schema):
    """SQL schema string used for creating tables."""
    return ",\n".join([f"{col} {datatype}" for col, datatype in schema])


def get_columns(schema):
    """comma-separated list of columns for running queries."""
    return ", ".join([col for col, _ in schema])


queries = {
    "abstract": f"SELECT {get_columns(schemas['abstract'])} FROM mid.abstract",
    "affiliation": f"SELECT {get_columns(schemas['affiliation'])} FROM mid.affiliation",
    "affiliation_string_v2": f"SELECT {get_columns(schemas['affiliation_string_v2'])} FROM mid.affiliation_string_v2",
    "affiliation_unique_rows_view": f"SELECT {get_columns(schemas['affiliation_unique_rows_view'])} FROM mid.affiliation_unique_rows_view",
    "author": f"SELECT {get_columns(schemas['author'])} FROM mid.author WHERE author_id > 5000000000 AND merge_into_id IS NULL",
    "author_orcid": f"SELECT {get_columns(schemas['author_orcid'])} FROM mid.author_orcid",
    "citation": f"SELECT {get_columns(schemas['citation'])} FROM mid.citation",
    "citation_institutions_mv": f"SELECT {get_columns(schemas['citation_institutions_mv'])} FROM mid.citation_institutions_mv",
    "citation_papers_mv": f"SELECT {get_columns(schemas['citation_papers_mv'])} FROM mid.citation_papers_mv",
    "country": f"SELECT {get_columns(schemas['country'])} FROM mid.country",
    "continent": f"SELECT {get_columns(schemas['continent'])} FROM mid.continent",
    "domain": f"SELECT {get_columns(schemas['domain'])} FROM mid.domain",
    "field": f"SELECT {get_columns(schemas['field'])} FROM mid.field",
    "funder": f"SELECT {get_columns(schemas['funder'])} FROM mid.funder where merge_into_id IS NULL",
    "institution": f"SELECT {get_columns(schemas['institution'])} FROM mid.institution",
    "institution_type": f"SELECT {get_columns(schemas['institution_type'])} FROM mid.institution_type",
    "keyword": f"SELECT {get_columns(schemas['keyword'])} FROM mid.keyword",
    "language": f"SELECT {get_columns(schemas['language'])} FROM mid.language",
    "license": f"SELECT {get_columns(schemas['license'])} FROM mid.license",
    "publisher": f"SELECT {get_columns(schemas['publisher'])} FROM mid.publisher WHERE merge_into_id IS NULL",
    "ror": f"SELECT {get_columns(schemas['ror'])} FROM ins.ror_summary",
    "sdg": f"SELECT {get_columns(schemas['sdg'])} FROM mid.sdg",
    "source": f"""
        SELECT 
            journal_id AS source_id, 
            {', '.join([col for col in get_columns(schemas['source']).split(', ') if col != 'source_id'])} 
        FROM mid.journal WHERE merge_into_id IS NULL
    """,
    "source_type": f"SELECT {get_columns(schemas['source_type'])} FROM mid.source_type",
    "subfield": f"SELECT {get_columns(schemas['subfield'])} FROM mid.subfield",
    "topic": f"SELECT {get_columns(schemas['topic'])} FROM mid.topic",
    "work": f"""SELECT {get_columns(schemas['work'])} FROM mid.work WHERE merge_into_id IS NULL AND "year" IS NOT NULL""",
    "work_concept": f"SELECT {get_columns(schemas['work_concept'])} FROM mid.work_concept WHERE score > 0.3",
    "work_keyword": f"SELECT {get_columns(schemas['work_keyword'])} FROM mid.work_keyword",
    "work_sdg": f"SELECT {get_columns(schemas['work_sdg'])} FROM public.normalized_sdg_scores WHERE score > 0.4",
    "work_topic": f"SELECT {get_columns(schemas['work_topic'])} FROM mid.work_topic",
    "work_type": f"SELECT {get_columns(schemas['work_type'])} FROM mid.work_type",
    "work_fwci": f"SELECT {get_columns(schemas['work_fwci'])} FROM counts.work_fwci",
    "work_funder": f"SELECT {get_columns(schemas['work_funder'])} FROM mid.work_funder",
}


def create_tables(table_name, schema):
    """helper function to create a table and its staging table."""
    schema_sql = get_schema_sql(schema)
    with redshift_engine.connect() as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema_sql}
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}_staging (
                {schema_sql}
            )
            """
        )
    logger.info(f"tables {table_name} and {table_name}_staging created if not exists.")


def get_s3_key(entity):
    """generate S3 filename (key) for the given entity."""
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
    """delete the leftover S3 file."""
    logger.info(f"Deleting S3 file s3://{s3_bucket}/{s3_key}")
    s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    logger.info(f"Successfully deleted S3 file s3://{s3_bucket}/{s3_key}")


def record_redshift_table_update_timestamp(redshift_table_name):
    """in the postgres database, maintain a table which records the last time the redshift tables were synced"""
    from app import db
    from sqlalchemy import text

    log_table = 'public.redshift_sync_logs'
    now = datetime.datetime.now(datetime.timezone.utc)
    sq = f"""INSERT INTO {log_table} (redshift_table, updated_at) VALUES(:redshift_table_name, :now);"""
    logger.info(f"Updating sync table in postgres: {log_table}")
    db.session.execute(text(sq), {'redshift_table_name': redshift_table_name, 'now': now})
    db.session.commit()


def main(entity):
    schema = schemas.get(entity)
    query = queries.get(entity)
    if not schema or not query:
        raise ValueError(f"Entity {entity} not found in schemas and queries")

    create_tables(entity, schema)

    s3_key = get_s3_key(entity)

    start_time = time.time()
    export_postgres_to_s3(query, s3_key, entity)
    truncate_staging_table(redshift_engine, entity)
    copy_s3_to_redshift_staging(s3_key, redshift_engine, entity)
    replace_existing_data(redshift_engine, entity)
    delete_s3_file(s3_key)
    truncate_staging_table(redshift_engine, entity)
    try:
        record_redshift_table_update_timestamp(entity)
    except Exception as e:
        logger.error(f"An error occurred so skipping sync log update: {e}")
    logger.info(f"{entity} completed in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL script to copy data from PostgreSQL to Redshift via S3.")
    parser.add_argument("--entity", required=True, type=str,
                        help="The entity to process (e.g., affiliation, institution, work, work_concept).")
    args = parser.parse_args()

    entity_input = args.entity
    try:
        main(entity_input)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
