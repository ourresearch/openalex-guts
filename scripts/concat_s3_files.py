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

PART_SUFFIX = r'\d+_part_\d+$'
SKIP_FILES = ["PaperAbstractsInvertedIndex.txt"]
DUMP_DIR = "2021-12-06"

##  python -m scripts.concat_s3_files  data_dump_v1/2021-12-06/mag  data_dump_v1/2021-12-06/advanced  data_dump_v1/2021-12-06/nlp --delete
## heroku run --size=performance-l python -m scripts.concat_s3_files openalex-sandbox data_dump_v1/2021-12-06/mag  data_dump_v1/2021-12-06/advanced  data_dump_v1/2021-12-06/nlp --delete --threads=20

## takes about 15 minutes


_num_threads = 1

def upload_part(part, bucket, key, upload_id):
    s3 = boto3.session.Session().client('s3')

    resp = s3.upload_part_copy(
        Bucket=bucket,
        Key=key,
        PartNumber=part['part_num'],
        UploadId=upload_id,
        CopySource=part['source_part']
    )

    msg = "Setup S3 part #{}, with path: {}".format(part['part_num'],
                                                    part['source_part'])
    logger.debug("{}, got response: {}".format(msg, resp))

    # ceph doesn't return quoted etags
    etag = (resp['CopyPartResult']['ETag']
            .replace("'", "").replace("\"", ""))

    return {'ETag': etag, 'PartNumber': part['part_num']}


def _assemble_parts(self, s3):
    parts_mapping = []
    part_num = 0

    s3_parts = ["{}/{}".format(self.bucket, p[0])
                for p in self.parts_list if p[1] > MIN_S3_SIZE]

    local_parts = [p for p in self.parts_list if p[1] <= MIN_S3_SIZE]

    with concurrent.futures.ProcessPoolExecutor(max_workers=_num_threads) as pool:
        parts_mappings = pool.map(
            partial(upload_part, bucket=self.bucket, key=self.result_filepath, upload_id=self.upload_id),
            [{'part_num': part_num, 'source_part': source_part} for part_num, source_part in enumerate(s3_parts, 1)],
            chunksize=1
        )

    parts_mapping.extend([pm for pm in parts_mappings])

    # assemble parts too small for direct S3 copy by downloading them,
    # combining them, and then reuploading them as the last part of the
    # multi-part upload (which is not constrained to the 5mb limit)

    # Concat the small_parts into the minium size then upload
    # this way not to much data is kept in memory
    def get_small_parts(data):
        part_num, part = data
        small_part_count = len(part[1])

        logger.debug("Start sub-part #{} from {} files"
                     .format(part_num, small_part_count))

        small_parts = []
        for p in part[1]:
            try:
                small_parts.append(
                    s3.get_object(
                        Bucket=self.bucket,
                        Key=p[0]
                    )['Body'].read()
                )
            except Exception as e:
                logger.critical(
                    f"{e}: When getting {p[0]} from the bucket {self.bucket}")  # noqa: E501
                raise

        if len(small_parts) > 0:
            last_part = b''.join(small_parts)

            small_parts = None  # cleanup
            resp = s3.upload_part(Bucket=self.bucket,
                                  Key=self.result_filepath,
                                  PartNumber=part_num,
                                  UploadId=self.upload_id,
                                  Body=last_part)
            msg = "Finish sub-part #{} from {} files" \
                .format(part_num, small_part_count)
            logger.debug("{}, got response: {}".format(msg, resp))

            last_part = None
            # Handles both quoted and unquoted etags
            etag = resp['ETag'].replace("'", "").replace("\"", "")
            return {'ETag': etag,
                    'PartNumber': part_num}
        return {}

    data_to_thread = []
    for idx, data in enumerate(_chunk_by_size(local_parts,
                                              MIN_S3_SIZE * 2),
                               start=1):
        data_to_thread.append([part_num + idx, data])

    parts_mapping.extend(
        _threads(self.small_parts_threads,
                 data_to_thread,
                 get_small_parts)
    )

    # Sort part mapping by part number
    return sorted(parts_mapping, key=lambda i: i['PartNumber'])


MultipartUploadJob._assemble_parts = _assemble_parts


def concat_table(table, bucket_name, delete, dry_run):
    if not table["output_key"]:
        return

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

    tables = defaultdict(lambda: {'part_keys': [], 'output_key': "", "header_key": ""})

    for obj in bucket.list(base_prefix):
        if re.search(PART_SUFFIX, obj.key):
            table_prefix = re.sub(PART_SUFFIX, '', obj.key)
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
        if basename not in SKIP_FILES:
            table['output_key'] = output_key

    return tables.values()

def do_directory_cleanups(bucket_name):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)

    # do this before the listing
    try:
        print(f'{bucket_name}/data_dump_v1/{DUMP_DIR}/README.txt000')
        s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/README.txt').copy_from(
            CopySource=f'{bucket_name}/data_dump_v1/{DUMP_DIR}/README.txt000')
        s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/README.txt000').delete()
    except Exception as e:
        print("Tried to rename README.txt000 file, but there wasn't one")
        pass

    # do the listing
    print(f"Creating the LISTING.txt file at data_dump_v1/{DUMP_DIR}/LISTING.txt")
    my_string = ""
    for object_summary in my_bucket.objects.filter(Prefix=f"data_dump_v1/{DUMP_DIR}/"):
        filename = object_summary.key
        size_in_mb = round(my_bucket.Object(filename).content_length / (1000 * 1000))
        my_string += f"{filename:70}{size_in_mb:>10,d} MB\n"

    s3.Object(bucket_name, f"data_dump_v1/{DUMP_DIR}/LISTING.txt").put(Body=my_string.encode("utf-8"))

    # set content types
    print(f"Setting the content types for data_dump_v1/{DUMP_DIR}/README.txt file and data_dump_v1/{DUMP_DIR}/LISTING.txt")
    object = s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/README.txt')
    object.copy_from(CopySource={'Bucket': bucket_name,
                                 'Key': f'data_dump_v1/{DUMP_DIR}/README.txt'},
                     MetadataDirective="REPLACE",
                     ContentType="text/plain")
    object = s3.Object(bucket_name, f'data_dump_v1/{DUMP_DIR}/LISTING.txt')
    object.copy_from(CopySource={'Bucket': bucket_name,
                                 'Key': f'data_dump_v1/{DUMP_DIR}/LISTING.txt'},
                     MetadataDirective="REPLACE",
                     ContentType="text/plain")


def merge_in_headers(table, bucket_name):
    if not table["part_keys"]:
        return

    if not table["header_key"]:
        return

    s3_client = boto3.client('s3')

    header_key = table["header_key"]
    first_part_key = table["part_keys"][0]

    print(f"downloading header file {header_key}")
    header_object = s3_client.get_object(Bucket=bucket_name, Key=header_key)
    header_data = header_object['Body'].read().decode('utf-8')

    print(f"downloading first part file {first_part_key}")
    first_part_object = s3_client.get_object(Bucket=bucket_name, Key=first_part_key)
    first_part_data = first_part_object['Body'].read().decode('utf-8')

    print(f"merging {first_part_key}")
    merged_data = header_data + first_part_data

    print(f"uploading {first_part_key}")
    buffer_to_upload = io.BytesIO(merged_data.encode())
    s3_client.put_object(Body=buffer_to_upload, Bucket=bucket_name, Key=first_part_key)
    print(f"DONE MERGE for {first_part_key}")

    # then delete header
    print(f"DELETING HEADER {header_key}")
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, header_key).delete()


def run(bucket_name, prefixes, delete, dry_run, threads):

    tables = []
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)
    for prefix in prefixes:
        tables.extend(get_tables(bucket, prefix))

    for table in tables:
        merge_in_headers(table, bucket_name)

    global _num_threads
    _num_threads = threads

    for table in tables:
        concat_table(table, bucket_name, delete, dry_run)

    do_directory_cleanups(bucket_name)




if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('bucket', help='bucket contaning the table files')
    ap.add_argument('prefix', action='extend', nargs='+', help='base prefix for table files, not including the bucket name')
    ap.add_argument('--threads', '-t', nargs='?', type=int, default=1, help='number of tables to concatenate in parallel')
    ap.add_argument('--delete', default=False, action='store_true', help='delete the source files after concatenating')
    ap.add_argument('--dry-run', default=False, action='store_true', help='only report the files that would have been concatenated')

    parsed = ap.parse_args()
    run(parsed.bucket, parsed.prefix, parsed.delete, parsed.dry_run, parsed.threads)