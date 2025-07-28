import logging
import os
import subprocess
import time

"""
Run with: heroku local:run python -- -m scripts.export_affiliations
"""

logger = logging.getLogger(__name__)

postgres_db_url = os.getenv("POSTGRES_URL")

def export_postgres_to_s3(query, s3_key, chunk_size):
    """execute the query in chunks and copy the output to S3 via multiple CSV files."""
    offset = 0
    part_number = 1

    while True:
        paginated_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
        s3_key_with_part = f"{s3_key[:-4]}_part{part_number}.csv"  # Add part number to the S3 key

        command = f"""
        psql {postgres_db_url} -c "\\COPY ({paginated_query}) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',');" | aws s3 cp - s3://{s3_bucket}/{s3_key_with_part}
        """
        logger.info(f"Executing command to save part {part_number} to s3://{s3_bucket}/{s3_key_with_part}")
        start_time = time.time()
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            logger.error(f"Error during copy to S3 for entity part {part_number}: {stderr.decode('utf-8')}")
            raise RuntimeError(f"Failed to copy data to S3. Command: {command}")
        else:
            logger.info(f"Successfully copied data to S3 for entity part {part_number} in {time.time() - start_time:.2f} seconds")

        # check if we have exported all the data (no rows in this chunk)
        if process.returncode == 0 and len(stdout) == 0:
            logger.info("Finished exporting all parts for affiliations.")
            break

        # increment to the next part
        offset += chunk_size
        part_number += 1


if __name__ == "__main__":
    s3_bucket = f"openalex-sandbox/affiliations/{time.strftime('%Y-%m-%d')}"
    chunk_size = 4250000  # 850M / 200 parts = 4.25M rows per part
    query = "SELECT * from mid.affiliation ORDER BY paper_id, author_id"
    export_postgres_to_s3(query, s3_key="affiliations.csv", chunk_size=chunk_size)
