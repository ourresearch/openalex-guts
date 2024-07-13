import os
import logging

from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redshift_db_url = os.getenv("REDSHIFT_SERVERLESS_URL")
redshift_engine = create_engine(redshift_db_url)

# schemas
affiliation_schema = """
    paper_id BIGINT,
    author_id BIGINT,
    affiliation_id BIGINT,
    author_sequence_number INTEGER,
    original_author VARCHAR(65535),
    original_orcid VARCHAR(500)
"""

author_schema = """
    author_id BIGINT,
    display_name VARCHAR(65535),
    merge_into_id BIGINT
"""

citation_schema = """
    paper_id BIGINT,
    paper_reference_id BIGINT
"""

work_schema = """
    paper_id BIGINT,
    original_title VARCHAR(65535),
    doi_lower VARCHAR(500),
    journal_id BIGINT,
    merge_into_id BIGINT,
    publication_date VARCHAR(500),
    doc_type VARCHAR(500),
    genre VARCHAR(500),
    arxiv_id VARCHAR(500),
    is_paratext BOOLEAN,
    best_url VARCHAR(65535),
    best_free_url VARCHAR(65535),
    created_date VARCHAR(500)
"""

work_concept_schema = """
    paper_id BIGINT,
    field_of_study BIGINT
"""


def create_tables(table_name, schema):
    """helper function to create a table and its staging table."""
    with redshift_engine.connect() as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {schema}
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}_staging (
                {schema}
            )
            """
        )
    logger.info(f"Tables {table_name} and {table_name}_staging created successfully.")


if __name__ == "__main__":
    create_tables('affiliation', affiliation_schema)
    create_tables('author', author_schema)
    create_tables('citation', citation_schema)
    create_tables('work', work_schema)
    create_tables('work_concept', work_concept_schema)
