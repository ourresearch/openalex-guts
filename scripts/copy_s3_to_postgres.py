import argparse
from os import getenv
from contextlib import contextmanager
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
from time import time

from util import elapsed

postgres_url = urlparse(getenv("POSTGRES_URL"))

def new_postgres_connection(readonly=True):
    connection = psycopg2.connect(dbname=postgres_url.path[1:],
                                  user=postgres_url.username,
                                  password=postgres_url.password,
                                  host=postgres_url.hostname,
                                  port=postgres_url.port)
    connection.set_session(readonly=readonly, autocommit=True)
    return connection


@contextmanager
def get_postgres_cursor(readonly=False):
    connection = new_postgres_connection(readonly=readonly)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
      yield cursor
    finally:
      cursor.close()
      pass

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('table', help='tablename')
    # ap.add_argument('--threads', '-t', nargs='?', type=int, default=1, help='number of tables to concatenate in parallel')

    parsed = ap.parse_args()
    # run(parsed.bucket, parsed.prefix, parsed.delete, parsed.dry_run, parsed.threads)
    start_time = time()

    aws_access_key_id = getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = getenv("AWS_SECRET_ACCESS_KEY")
    table_name = parsed.table
    folder_name = table_name.replace(".", "_", 1)

    for i in range(0, 32):
        loop_time = time()
        print(f"loop number {i}")
        q2 = f"""analyze {table_name};
                SELECT reltuples::bigint AS estimate
                FROM   pg_class
                WHERE  oid = '{table_name}'::regclass;
                """
        print(q2)
        q = f"""SELECT aws_s3.table_import_from_s3(
           '{table_name}',
           '', 
           '(delimiter $$|$$, null $$$$)',
           aws_commons.create_s3_uri(
                   'openalex-sandbox',
                   'export-rds/{folder_name}/{i:0>4}_part_00',
                   'us-east-1'
                ),
           aws_commons.create_aws_credentials(
           '{aws_access_key_id}', '{aws_secret_access_key}', '')
        );"""
        print(q)
        with get_postgres_cursor() as cur:
            cur.execute(q2)
            rows = cur.fetchall()
            print(f"done, {rows}")
            cur.execute(q)

        print(f"loop time {elapsed(loop_time)}s, per loop time {elapsed(start_time)/(i+1)}s, total_time {elapsed(start_time)}s")

    # final print of size
    with get_postgres_cursor() as cur:
        cur.execute(q2)
        rows = cur.fetchall()
        print(f"done, {rows}")
        print(f"loop time {elapsed(loop_time)}s, total_time {elapsed(start_time)}s")
