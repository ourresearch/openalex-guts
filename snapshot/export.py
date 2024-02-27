# 1. back up current snapshot
#   edit snapshot/backup.py to set the last snapshot date
#   $ python3 -m snapshot.backup
#
# 2. export merged ids
#   $ bash ./snapshot/export_merged_ids.sh
#
# 3. run this script to creates the new contents of s3://openalex/data/ in a local directory ${data_dir}
#   $ python3 -m snapshot.export
#   "dumping entity rows to local data dir ${data_dir}"
#
# 4. update release notes
#   date the current release notes
#   in files-for-datadumps/standard-format/RELEASE_NOTES.txt, change "Next Release" to "RELEASE YYYY-MM-DD"
#   $ git add files-for-datadumps/RELEASE_NOTES.txt
#   $ git commit -m "added YYYY-MM-DD release notes"
#
# 5. upload to S3 for QA (optional)
#   aws s3 sync ${data_dir}/..  s3://openalex-sandbox/snapshot-yyyy-mm-dd-staging
#
# 6. upload approved copy to s3
#   set credentials for s3://openalex (separate from s3://openalex-sandbox)
#   delete existing files: aws s3 rm --recursive s3://openalex/data/
#   browse to data folder (ex ~/data/2023_09_20), and run: aws s3 sync . s3://openalex/data
#   browse to files-for-datadumps/standard-format and run: aws s3 cp RELEASE_NOTES.txt s3://openalex/RELEASE_NOTES.txt
#   check result at: https://openalex.s3.amazonaws.com/browse.html


from datetime import datetime
import gzip
import json
import multiprocessing as mp
import os
import time

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import redis

from app import ELASTIC_URL
from app import (
    AUTHORS_INDEX,
    CONCEPTS_INDEX,
    DOMAINS_INDEX,
    FIELDS_INDEX,
    FUNDERS_INDEX,
    INSTITUTIONS_INDEX,
    PUBLISHERS_INDEX,
    SOURCES_INDEX,
    SUBFIELDS_INDEX,
    TOPICS_INDEX,
    WORKS_INDEX,
)

data_dir = os.path.join(os.path.expanduser('~'), 'data', datetime.now().strftime("%Y_%m_%d"))
print(f"data directory is {data_dir}")

# Configure Elasticsearch client
es = Elasticsearch([ELASTIC_URL])

# Configure redis client
r = redis.Redis(host='localhost', port=6379, db=2)

entities_to_indices = {
    "works": WORKS_INDEX,
    "authors": AUTHORS_INDEX,
    "concepts": CONCEPTS_INDEX,
    "funders": FUNDERS_INDEX,
    "institutions": INSTITUTIONS_INDEX,
    "publishers": PUBLISHERS_INDEX,
    "sources": SOURCES_INDEX,
    "topics": TOPICS_INDEX,
    "domains": DOMAINS_INDEX,
    "fields": FIELDS_INDEX,
    "subfields": SUBFIELDS_INDEX,
}


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


def create_search_query(index_name, d, search_after=None):
    page_size = 1000
    s = Search(using=es, index=index_name).query("term", updated_date=d)
    s = s.sort(*["-cited_by_count", "id"])
    s = s.source(excludes=['_source', 'embeddings', 'fulltext', 'abstract', 'vector_embedding', 'version', '@version', '@timestamp'])
    if search_after:
        s = s.extra(size=page_size, search_after=search_after)
    else:
        s = s.extra(size=page_size)
    s = s.params(preference=d)
    return s


def export_date(args):
    index_name, entity_type, d = args
    max_file_size = 5 * 1024 ** 3  # 5GB uncompressed
    count = 0
    index_id_prefix = f"https://openalex.org/{entity_type[0].upper()}"
    date_dir = os.path.join(data_dir, entity_type, f"updated_date={d}")

    part_file_number = 0
    part_file = None  # Initialize as None
    total_size = 0
    s = create_search_query(index_name, d)
    response = s.execute()

    while len(response.hits) > 0:
        if part_file is None:  # Only create dir and file if there are hits
            os.makedirs(date_dir, exist_ok=True)
            part_file = gzip.open(f'{date_dir}/part_{str(part_file_number).zfill(3)}.gz', 'wt')

        for hit in response:
            record_id = hit.id
            # convert to integer
            try:
                record_id = int(record_id.replace(index_id_prefix, ""))
            except ValueError:
                print(f"Skipping record {record_id}. Not an integer.")
                continue
            if r.sadd('record_ids', record_id):
                count += 1
                record = hit.to_dict()

                # handle truncated authors
                if entity_type == "works" and record.get("authorships") and record.get('authorships_full'):
                    record["authorships"] = record["authorships_full"]
                    del record["authorships_full"]
                    if record.get("is_authors_truncated"):
                        del record["is_authors_truncated"]

                # handle abstract inverted index
                if (
                        entity_type == "works"
                        and record.get("abstract_inverted_index")
                ):
                    record["abstract_inverted_index"] = json.loads(
                        record["abstract_inverted_index"]
                    )
                    record["abstract_inverted_index"] = record["abstract_inverted_index"].get("InvertedIndex")

                line = json.dumps(record) + '\n'
                line_size = len(line.encode('utf-8'))

                # If this line will make the file exceed the max size, close the current file and open a new one
                if total_size + line_size > max_file_size:
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
        s = create_search_query(index_name, d, search_after=last_hit_sort)
        response = s.execute()

    if part_file is not None:  # If file was created, close it
        part_file.close()


def export_entity(index_name, entity_type):
    distinct_updated_dates = get_distinct_updated_dates(index_name)

    args_for_export = ((index_name, entity_type, d) for d in distinct_updated_dates)

    with mp.Pool(12) as p:
        _ = list(p.imap_unordered(export_date, args_for_export))


def make_manifests():
    remote_data_dir = 's3://openalex/data'
    for entity_type in entities_to_indices.keys():
        print(f"making manifest for {entity_type}")
        total_content_length = 0
        total_record_count = 0

        entity_dir = os.path.join(data_dir, entity_type)
        manifest = os.path.join(entity_dir, "manifest")

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
                        f.write(
                            f"    {{\"url\": \"{s3_url}\", \"meta\": {{ \"content_length\": {content_length}, \"record_count\": {record_count} }} }},\n")

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
    for entity, index in entities_to_indices.items():
        start_time = time.time()
        r.flushdb()
        export_entity(index, entity)
        end_time = time.time()
        print(f"Total time: {end_time - start_time} seconds")
    r.flushdb()
    make_manifests()
