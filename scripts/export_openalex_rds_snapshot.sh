#!/bin/bash

data_dir=$(mktemp -d)/data

echo "dumping entity rows to local data dir ${data_dir}"

#heroku local:run python -m scripts.dump_snapshot_rows_rds $data_dir

get_distinct_updated_dates() {
    table_name=$1
    local -n dates=$2

    echo "get distinct updated dates for ${table_name}"

    dates=( $(
        psql $OPENALEX_DB -q -t  -c \
        "select distinct updated::date from ${table_name} \
        where merge_into_id is null and json_save is not null"\
    ) )
}

export_table() {
    table_name=$1
    entity_type=$2

    local updated_dates
    get_distinct_updated_dates $table_name updated_dates

    for d in ${updated_dates[@]}
    do
        date_dir="${data_dir}/${entity_type}/updated_date=${d}"
        mkdir -p $date_dir
        echo "dumping ${entity_type} updated ${d} to ${date_dir}"

        psql $OPENALEX_DB -c "\\copy ( \
                select json_save from $table_name \
                where updated >= '$d' and updated < '$d'::date + '1 day'::interval \
                and merge_into_id is null and json_save is not null \
                limit 2000000 \
            ) to $date_dir/$entity_type"

    done
}

# dump one big file for each entity type/date

export_table 'mid.json_works' 'works'
export_table 'mid.json_authors' 'authors'
export_table 'mid.json_venues' 'venues'
export_table 'mid.json_institutions' 'institutions'
export_table 'mid.json_concepts' 'concepts'

# split the mega files

for f in $(ls ${data_dir}/*/*/*)
do
    echo "splitting $f into 5GB chunks"
    part_prefix=$(dirname $f)/part_
    split --numeric-suffixes --line-bytes=5G --suffix-length=3 $f $part_prefix

    echo "compressing parts"
    gzip $part_prefix*

    echo "removing $f"
    rm $f
done

# make manifests
for entity in works authors venues institutions concepts
do
    ls $data_dir/$entity/*/* |
    sed "s|$data_dir|s3://openalex/data|" |
    jq -s -R 'split("\n") | map(select(length > 0))  | sort | map({url: .}) | {entries: .}' |
    sed -z 's/\n\s*"url"/"url"/g' |
    sed -z 's/\.gz"\s*\n\s*/.gz"/g' > $data_dir/$entity/manifest
done
