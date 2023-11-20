#!/bin/bash

data_dir="$HOME/data/$(date +%Y_%m_%d)"
mkdir -p "${data_dir}/merged_ids"
echo "Created local data directory: ${data_dir}/merged_ids"

echo "removing old files from S3"
aws s3 rm s3://openalex-sandbox/snapshot-merged-ids/merged_ids --recursive

echo "exporting merge ids from database"
psql $OPENALEX_DB -f ./snapshot/export_merge_ids.sql

echo "syncing files from S3 to local directory"
aws s3 sync s3://openalex-sandbox/snapshot-merged-ids/merged_ids "${data_dir}/merged_ids"

echo "gzipping files in local directory"
gzip ${data_dir}/merged_ids/*/*.csv
