from datetime import datetime
import gzip
import json
import multiprocessing as mp
import os
import time

import boto3
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import redis

from app import ELASTIC_URL

data_dir = os.path.join(os.path.expanduser('~'), 'data', datetime.now().strftime("%Y_%m_%d"))
print(f"data directory is {data_dir}")

# Configure AWS S3 client
s3 = boto3.client('s3')

# Configure Elasticsearch client
es = Elasticsearch([ELASTIC_URL])

# Configure redis client
r = redis.Redis(host='localhost', port=6379, db=2)


def get_distinct_updated_dates(index_name):
    print(f"get distinct changed dates for {index_name}")

    # Define the query to aggregate on changed_date
    query = {
        "size": 0,  # we don't need the actual documents
        "aggs": {
            "distinct_dates": {
                "date_histogram": {
                    "field": "updated_date",
                    "calendar_interval": "day",  # aggregate into buckets by day
                }
            }
        }
    }

    # Execute the search query
    response = es.search(index=index_name, body=query)

    # Extract the bucket keys
    dates = [bucket["key_as_string"] for bucket in response["aggregations"]["distinct_dates"]["buckets"]]

    # Convert the dates to yyyy-mm-dd format
    dates = [date.split("T")[0] for date in dates]

    # Sort the dates newest to oldest
    dates.sort(reverse=True)

    return dates


def export_date(args):
    index_name, entity_type, d = args
    MAX_FILE_SIZE = 5 * 1024 ** 3  # 5GB uncompressed
    PAGE_SIZE = 1000  # Set a page size for the search after queries
    count = 0
    date_dir = os.path.join(data_dir, entity_type, f"updated_date={d}")

    part_file_number = 0
    part_file = None  # Initialize as None
    total_size = 0

    s = Search(using=es, index=index_name).query("term", updated_date=d)
    s = s.sort(*["-cited_by_count", "id"])
    s = s.source(excludes=['_source', 'fulltext', 'abstract', 'version', '@version', '@timestamp'])
    s = s.extra(size=PAGE_SIZE)
    response = s.execute()

    while len(response.hits) > 0:
        if part_file is None:  # Only create dir and file if there are hits
            os.makedirs(date_dir, exist_ok=True)
            part_file = gzip.open(f'{date_dir}/part_{str(part_file_number).zfill(3)}.gz', 'wt')

        for hit in response:
            record_id = hit.id
            # convert to integer
            try:
                record_id = int(record_id.replace("https://openalex.org/W", ""))
            except ValueError:
                print(f"Skipping record {record_id}. Not an integer.")
                continue
            if r.sadd('record_ids', record_id):
                count += 1
                line = json.dumps(hit.to_dict()) + '\n'
                line_size = len(line.encode('utf-8'))

                # If this line will make the file exceed the max size, close the current file and open a new one
                if total_size + line_size > MAX_FILE_SIZE:
                    part_file.close()
                    part_file_number += 1
                    part_file = gzip.open(f'{date_dir}/part_{str(part_file_number).zfill(3)}.gz', 'wt')
                    total_size = 0

                if count % 10000 == 0:
                    print(f"{entity_type} {d} {count}")

                part_file.write(line)
                total_size += line_size
            else:
                print(f"Skipping record {record_id}. Already in dataset.")

        # Get the last document's sort value and use it for the search_after parameter
        last_hit_sort = response.hits.hits[-1]['sort']
        s = Search(using=es, index=index_name).query("term", updated_date=d)
        s = s.sort(*["-cited_by_count", "id"])
        s = s.source(excludes=['_source', 'fulltext', 'abstract'])
        s = s.extra(size=PAGE_SIZE, search_after=last_hit_sort)
        response = s.execute()

    if part_file is not None:  # If file was created, close it
        part_file.close()


def export_entity(index_name, entity_type):
    distinct_updated_dates = get_distinct_updated_dates(index_name)
    with mp.Pool(mp.cpu_count()) as p:
        p.map(export_date, [(index_name, entity_type, d) for d in distinct_updated_dates])


def make_manifests():
    remote_data_dir = 'openalex/data'  # replace with your s3 bucket path
    for entity_type in ['concepts', 'institutions', 'sources']:
        total_content_length = 0
        total_record_count = 0

        entity_dir = os.path.join(data_dir, entity_type)
        manifest = os.path.join(entity_dir, "manifest.json")

        with open(manifest, 'w') as f:
            f.write("{\n  \"entries\": [\n")

        for root, dirs, files in os.walk(entity_dir):
            for file in files:
                if file.endswith('.gz'):
                    file_path = os.path.join(root, file)
                    s3_url = os.path.join(remote_data_dir, entity_type, os.path.relpath(file_path, entity_dir))
                    content_length = os.path.getsize(file_path)
                    record_count = sum(1 for line in gzip.open(file_path, 'rt'))

                    total_content_length += content_length
                    total_record_count += record_count

                    with open(manifest, 'a') as f:
                        f.write(f"    {{\"url\": \"{s3_url}\", \"meta\": {{ \"content_length\": {content_length}, \"record_count\": {record_count} }} }},\n")

        with open(manifest, 'rb+') as f:
            f.seek(-2, os.SEEK_END)
            f.truncate()

        with open(manifest, 'a') as f:
            f.write("\n  ],\n")
            f.write("  \"meta\": {\n")
            f.write(f"    \"content_length\": {total_content_length},\n")
            f.write(f"    \"record_count\": {total_record_count}\n")
            f.write("  }\n")
            f.write("}\n")


if __name__ == "__main__":
    start_time = time.time()
    r.flushdb()
    export_entity('works-v18-*,-*invalid-data', 'works')
    end_time = time.time()
    print(f"Total time: {end_time - start_time} seconds")
    # make_manifests()

# Replace the arguments with the appropriate index names and json field names
# export_entity('index_name_for_concepts', 'concepts', 'json_save')
# export_table('index_name_for_funders', 'funders', 'json_save')
# export_table('index_name_for_institutions', 'institutions', 'json_save')
# export_table('index_name_for_publishers', 'publishers', 'json_save')
# export_table('index_name_for_sources', 'sources', 'json_save')
# export_table('index_name_for_authors', 'authors', 'json_save')
# export_table('index_name_for_works', 'works', 'json_save_with_abstract')

# make_manifests()
