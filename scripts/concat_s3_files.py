import argparse
import concurrent.futures
import re
from collections import defaultdict
from functools import partial
from os import getenv

import boto
from s3_concat import S3Concat

from app import logger

PART_SUFFIX = r'\d+_part_\d+$'

##  python -m scripts.concat_s3_files openalex-sandbox export/mag export/advanced export/nlp --delete
##  heroku run --size=performance-l python -m scripts.concat_s3_files openalex-sandbox export/mag export/advanced export/nlp --delete


def concat_table(table, bucket_name, delete, dry_run):
    job = S3Concat(
        bucket=bucket_name,
        key=table['output_key'],
        min_file_size=None,
        s3_client_kwargs={
            'aws_access_key_id': getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': getenv('AWS_SECRET_ACCESS_KEY')
        }
    )

    for part_key in table['part_keys']:
        job.add_files(part_key)

    logger.info(f'concatenating these files into s3://{bucket_name}/{table["output_key"]}')
    for part_file in job.all_files:
        logger.info(f'  s3://{bucket_name}/{part_file[0]}')

    if not dry_run:
        job.concat()

    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)

    if delete:
        for part_file in job.all_files:
            logger.info(f'deleting s3://{bucket_name}/{part_file[0]}')
            if not dry_run:
                bucket.delete_key(part_file[0])

    return table


def get_tables(bucket, base_prefix):
    if not base_prefix.endswith('/'):
        base_prefix = f'{base_prefix}/'

    tables = defaultdict(lambda: {'part_keys': []})

    for obj in bucket.list(base_prefix):
        if re.search(PART_SUFFIX, obj.key):
            table_prefix = re.sub(PART_SUFFIX, '', obj.key)
            tables[table_prefix]['part_keys'].append(obj.key)

    if not tables:
        logger.info(f'found no table part files in s3://{bucket.name}/{base_prefix}')

    for table_prefix, table in tables.items():
        basename = table_prefix.split('/')[-1]
        output_key = f'{base_prefix}{basename}'
        table['output_key'] = output_key

    return tables.values()


def run(bucket_name, prefixes, delete, dry_run, threads):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)

    tables = []
    for prefix in prefixes:
        tables.extend(get_tables(bucket, prefix))

    with concurrent.futures.ProcessPoolExecutor(max_workers=threads) as pool:
        mapped = pool.map(
            partial(concat_table, delete=delete, dry_run=dry_run, bucket_name=bucket_name),
            tables,
            chunksize=1
        )

    return [m for m in mapped]


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('bucket', help='bucket contaning the table files')
    ap.add_argument('prefix', action='extend', nargs='+', help='base prefix for table files, not including the bucket name')
    ap.add_argument('--threads', '-t', nargs='?', type=int, default=1, help='number of tables to concatenate in parallel')
    ap.add_argument('--delete', default=False, action='store_true', help='delete the source files after concatenating')
    ap.add_argument('--dry-run', default=False, action='store_true', help='only report the files that would have been concatenated')

    parsed = ap.parse_args()
    run(parsed.bucket, parsed.prefix, parsed.delete, parsed.dry_run, parsed.threads)
