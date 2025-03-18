import tempfile
import argparse
import concurrent.futures
import re
from collections import defaultdict
from functools import partial
from os import getenv
import io

import boto
import boto3
from s3_concat import MultipartUploadJob
from s3_concat import S3Concat
from s3_concat.utils import _threads, _chunk_by_size, MIN_S3_SIZE

from app import logger

PART_SUFFIX = r'\.txt(?:_part\d+)?$'
SKIP_FILE_PREFIX = "PaperAbstractsInvertedIndex"
DUMP_DIR = "2022-06-13"

##  python -m scripts.concat_s3_files  data_dump_v1/2022-04-07/mag  data_dump_v1/2022-04-07/advanced  data_dump_v1/2022-04-07/nlp --delete
## heroku run --size=performance-l python -m scripts.concat_s3_files openalex-mag-format data_dump_v1/2022-04-07/mag  data_dump_v1/2022-04-07/advanced  data_dump_v1/2022-04-07/nlp --delete --threads=20

## takes about 15 minutes

def concat_table(table, bucket_name, delete, dry_run):
    if not table["output_key"]:
        return

    work_dir = tempfile.mkdtemp()
    work_file = work_dir + '/' + table["output_key"].split('/')[-1]

    s3 = boto3.client('s3')

    with open(work_file, 'ab') as f:
        if 'header_key' in table:
            logger.info(f"append {table['header_key']} to {work_file}")
            s3.download_fileobj(bucket_name, table['header_key'], f)
        for part_key in table.get('part_keys', []):
            logger.info(f"append {part_key} to {work_file}")
            s3.download_fileobj(bucket_name, part_key, f)

    logger.info(f'uploading {work_file} to {table["output_key"]}')
    s3.upload_file(work_file, bucket_name, table["output_key"])

    return table


def get_tables(bucket, base_prefix):
    if not base_prefix.endswith('/'):
        base_prefix = f'{base_prefix}/'

    tables = defaultdict(lambda: {'part_keys': [], 'output_key': "", "header_key": ""})

    for obj in bucket.list(base_prefix):
        key_without_base_prefix = str(obj.key)
        key_without_base_prefix = key_without_base_prefix.replace(base_prefix, "")
        if "/" in key_without_base_prefix:
            table_prefix = re.sub(PART_SUFFIX, '.txt', obj.key)
            if "HEADER_" in obj.key:
                table_prefix = table_prefix.replace("HEADER_", "")
                tables[table_prefix]['header_key'] = obj.key
            else:
                tables[table_prefix]['part_keys'].append(obj.key)

    if not tables:
        logger.info(f'found no table part files in s3://{bucket.name}/{base_prefix}')

    for table_prefix, table in tables.items():
        basename = table_prefix.split('/')[-1]
        output_key = f'{base_prefix}{basename}'
        if SKIP_FILE_PREFIX not in basename:
            table['output_key'] = output_key

    return tables.values()

def do_directory_cleanups(bucket_name):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)

    # do this before the listing
    #try:
    #    print(f'{bucket_name}/data_dump_v1/{DUMP_DIR}/README.txt000')
    #    s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/README.txt').copy_from(
    #        CopySource=f'{bucket_name}/data_dump_v1/{DUMP_DIR}/README.txt000')
    #    s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/README.txt000').delete()
    #except Exception as e:
    #    print("Tried to rename README.txt000 file, but there wasn't one")
    #    pass

    # do the listing
    print(f"Creating the LISTING.txt file at data_dump_v1/{DUMP_DIR}/LISTING.txt")
    my_string = ""
    for object_summary in my_bucket.objects.filter(Prefix=f"data_dump_v1/{DUMP_DIR}/"):
        filename = object_summary.key
        size_in_mb = round(my_bucket.Object(filename).content_length / (1024 * 1024))
        if size_in_mb == 0:
            size_in_kb = round(my_bucket.Object(filename).content_length / 1024)
            my_string += f"{filename:70}{size_in_kb:>10,d} KB\n"
        else:
            my_string += f"{filename:70}{size_in_mb:>10,d} MB\n"

    s3.Object(bucket_name, f"data_dump_v1/{DUMP_DIR}/LISTING.txt").put(Body=my_string.encode("utf-8"))

    # set content types
    print(f"Setting the content types for data_dump_v1/{DUMP_DIR}/LISTING.txt")
    try:
        object = s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/LISTING.txt')
        object.copy_from(CopySource={'Bucket': bucket_name,
                                     'Key': f'data_dump_v1/{DUMP_DIR}/LISTING.txt'},
                         MetadataDirective="REPLACE",
                         ContentType="text/plain")
    except Exception as e:
        print(f"error failed to copy listing.txt {e} {e.message}")
        print(f"continuing anyway")


def run(bucket_name, prefixes, delete, dry_run, threads):
    tables = []
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)

    for prefix in prefixes:
        tables.extend(get_tables(bucket, prefix))

    with concurrent.futures.ProcessPoolExecutor(max_workers=threads) as pool:
        pool.map(
            partial(concat_table, bucket_name=bucket_name, delete=delete, dry_run=dry_run),
            tables,
            chunksize=1
        )

    #do_directory_cleanups(bucket_name)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('bucket', help='bucket containing the table files')
    ap.add_argument('prefix', action='extend', nargs='+', help='base prefix for table files, not including the bucket name')
    ap.add_argument('--threads', '-t', nargs='?', type=int, default=1, help='number of tables to concatenate in parallel')
    ap.add_argument('--delete', default=False, action='store_true', help='delete the source files after concatenating')
    ap.add_argument('--dry-run', default=False, action='store_true', help='only report the files that would have been concatenated')

    parsed = ap.parse_args()
    run(parsed.bucket, parsed.prefix, parsed.delete, parsed.dry_run, parsed.threads)
