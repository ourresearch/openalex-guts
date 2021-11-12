import argparse
import re
from os import getenv

import boto
from s3_concat import S3Concat

from app import logger

PART_SUFFIX = r'\d+_part_\d+$'

##  python -m scripts.concat_s3_files openalex-sandbox export/mag export/advanced export/nlp --delete
##  heroku run --size=performance-l python -m scripts.concat_s3_files openalex-sandbox export/mag export/advanced export/nlp --delete

def concat_files(bucket, bucket_name, prefix, output, delete, dry_run):
    job = S3Concat(
        bucket=bucket_name,
        key=output,
        min_file_size=None,
        s3_client_kwargs={
            'aws_access_key_id': getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': getenv('AWS_SECRET_ACCESS_KEY')
        }
    )

    job.add_files(prefix)

    logger.info(f'concatenating these files into s3://{bucket_name}/{output}')
    for part_file in job.all_files:
        logger.info(f'  s3://{bucket_name}/{part_file[0]}')

    if not dry_run:
        job.concat()

    if delete:
        for part_file in job.all_files:
            logger.info(f'deleting s3://{bucket_name}/{part_file[0]}')
            if not dry_run:
                bucket.delete_key(part_file[0])


def concat_prefix(bucket_name, base_prefix, delete, dry_run):
    if not base_prefix.endswith('/'):
        base_prefix = f'{base_prefix}/'

    logger.info(f'concatenating table files in s3://{bucket_name}/{base_prefix}')
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)

    table_prefixes = set()

    for obj in bucket.list(base_prefix):
        if re.search(PART_SUFFIX, obj.key):
            table_prefix = re.sub(PART_SUFFIX, '', obj.key)
            table_prefixes.add(table_prefix)

    if not table_prefixes:
        logger.info(f'found no table part files in s3://{bucket_name}/{base_prefix}')

    for table_prefix in table_prefixes:
        basename = table_prefix.split('/')[-1]
        output_key = f'{base_prefix}{basename}'
        concat_files(bucket, bucket_name, table_prefix, output_key, delete, dry_run)


def run(bucket_name, prefixes, delete, dry_run):
    for prefix in prefixes:
        concat_prefix(bucket_name, prefix, delete, dry_run)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('bucket', help='bucket contaning the table files')
    ap.add_argument('prefix', action='extend', nargs='+', help='base prefix for table files, not including the bucket name')
    ap.add_argument('--delete', default=False, action='store_true', help='delete the source files after concatenating')
    ap.add_argument('--dry-run', default=False, action='store_true', help='only report the files that would have been concatenated')

    parsed = ap.parse_args()
    run(bucket_name=parsed.bucket, prefixes=parsed.prefix, delete=parsed.delete, dry_run=parsed.dry_run)
