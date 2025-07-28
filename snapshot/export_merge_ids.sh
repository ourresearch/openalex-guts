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
gzip "${data_dir}/merged_ids/*/*.csv"

echo "creating Redshift manifest files"
create_manifest_file() {
    local entity_name="$1"
    local manifest_file="${data_dir}/merged_ids/${entity_name}/manifest.json"
    local s3_prefix="s3://openalex-sandbox/snapshot-merged-ids/merged_ids/${entity_name}"

    echo "Creating manifest for ${entity_name}"
    echo '{"entries": [' > "${manifest_file}"

    local first_entry=true
    for file in "${data_dir}/merged_ids/${entity_name}"/*.csv.gz; do
        if [[ -f "$file" ]]; then
            local filename
            filename=$(basename "$file")
            local s3_url="${s3_prefix}/${filename}"

            local file_size
            file_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
            local record_count
            record_count=$(($(zcat "$file" | wc -l) - 1))

            if [[ "$first_entry" == false ]]; then
                echo ',' >> "${manifest_file}"
            fi

            echo -n "    {\"url\": \"${s3_url}\", \"meta\": {\"content_length\": ${file_size}, \"record_count\": ${record_count}}}" >> "${manifest_file}"
            first_entry=false
        fi
    done

    echo '' >> "${manifest_file}"
    echo ']}' >> "${manifest_file}"

    if [[ -f "${manifest_file}" ]]; then
        aws s3 cp "${manifest_file}" "${s3_prefix}/manifest.json"
        echo "Uploaded manifest for ${entity_name} to ${s3_prefix}/manifest.json"
    fi
}

entities=("works" "authors" "sources" "institutions" "concepts" "funders" "publishers")
for entity in "${entities[@]}"; do
    if [[ -d "${data_dir}/merged_ids/${entity}" ]]; then
        create_manifest_file "${entity}"
    fi
done
