#!/bin/bash

# creates the contents of s3://openalex/data/ in a local temp directory
# after running,
#   replace s3 data contents
#   run /sql/export_merge_ids.sql
#   copy s3://openalex-sandbox/snapshot-merged-ids/merged_ids to s3://openalex/data/ (find out how to export directly to s3://openalex/to skip this step
#   update s3://openalex/RELEASE_NOTES.txt

data_dir=$(mktemp -d)/data

echo "dumping entity rows to local data dir ${data_dir}"

get_distinct_updated_dates() {
    table_name=$1
    json_field_name=$2
    updated_field_name=$3
    local -n dates=$4

    echo "get distinct updated dates for ${table_name}"

    dates=( $(
        psql $OPENALEX_DB -q -t  -c \
        "select distinct ${updated_field_name}::date from ${table_name} \
        where merge_into_id is null and ${json_field_name} is not null"\
    ) )
}

export_table() {
    table_name=$1
    entity_type=$2
    json_field_name=$3
    updated_field_name=$4

    local updated_dates
    get_distinct_updated_dates $table_name $json_field_name $updated_field_name updated_dates

    for d in ${updated_dates[@]}
    do
        date_dir="${data_dir}/${entity_type}/updated_date=${d}"
        mkdir -p $date_dir
        echo "dumping ${entity_type} ${json_field_name} updated ${d} to ${date_dir}"
        mega_file="$date_dir/$entity_type"

        psql $OPENALEX_DB -c "\\copy ( \
                select ${json_field_name} from $table_name \
                    where ${updated_field_name} >= '$d'::date and ${updated_field_name} < ('$d'::date + interval '1 day')::date \
                and merge_into_id is null and ${json_field_name} is not null \
            ) to $mega_file"


        echo "splitting $mega_file into 5GB chunks"

        part_prefix=$date_dir/part_
        split --numeric-suffixes --line-bytes=5G --suffix-length=3 $mega_file $part_prefix

        echo "removing $mega_file"
        rm $mega_file

        echo "compressing part files"
        gzip $part_prefix*

    done
}

export_table 'mid.json_concepts' 'concepts' 'json_save' 'updated'
export_table 'mid.json_institutions' 'institutions' 'json_save' 'updated'
export_table 'mid.json_venues' 'venues' 'json_save' 'updated'
export_table 'mid.json_authors_2022_06_09' 'authors' 'json_save' 'updated_date'
export_table 'mid.json_works_2022_06_09' 'works' 'json_save_with_abstract' 'updated_date'

# make manifests
for entity in venues institutions concepts
do
    ls $data_dir/$entity/*/* |
    sed "s|$data_dir|s3://openalex/data|" |
    jq -s -R 'split("\n") | map(select(length > 0))  | sort | map({url: .}) | {entries: .}' |
    sed -z 's/\n\s*"url"/"url"/g' |
    sed -z 's/\.gz"\s*\n\s*/.gz"/g' > $data_dir/$entity/manifest
done
