#!/bin/bash

# 1. back up current snapshot
#   $ aws s3 sync s3://openalex s3://openalex-sandbox/snapshot-backups/openalex-jsonl/current-date-yyyy-mm-dd

# 2. run this script to creates the new contents of s3://openalex/data/ in a local temp directory ${data_dir}
#   $ ./scripts/export_openalex_rds_snapshot.sh
#   "dumping entity rows to local data dir ${data_dir}"

# 3. export merged ids
#   # 3.1 remove old ones
#   $ aws s3 rm s3://openalex-sandbox/snapshot-merged-ids/merged_ids --recursive

#   # 3.2 export the new list
#   run /sql/export_merge_ids.sql in your favorite client

#   # 3.3 copy the result to your local snapshot, gzip the files
#   aws s3 sync s3://openalex-sandbox/snapshot-merged-ids/merged_ids ${data_dir}/merged_ids
#   gzip ${data_dir}/merged_ids/*/*.csv

# 4. make manifests somehow
#    like s3://openalex-sandbox/snapshot-backups/openalex-jsonl/2022-05-12/data/authors/manifest

# 5. add txt files and browse page
#   date the current release notes
#   in files-for-datadumps/standard-format/RELEASE_NOTES.txt, change "Next Release" to "RELEASE YYYY-MM-DD"
#   $ git add in files-for-datadumps/RELEASE_NOTES.txt
#   $ git commit -m "added YYYY-MM-DD release notes"
#   cp files-for-datadumps/standard-format/*.txt ${data_dir}/..
#
# 5. upload to 3 for QA
#   aws s3 sync ${data_dir}/..  s3://openalex-sandbox/snapshot-yyyy-mm-dd-staging

data_dir=$(mktemp -d)/data
today_yyyy_mm_dd=$(date +%Y-%m-%d)

echo "dumping entity rows to local data dir ${data_dir}"

get_distinct_updated_dates() {
    table_name=$1
    local -n dates=$2

    echo "get distinct updated dates for ${table_name}"

    dates=( $(
        psql $OPENALEX_DB -q -t  -c "select distinct updated::date from ${table_name}"
    ) )
}

export_table() {
    table_name=$1
    entity_type=$2
    json_field_name=$3

    table_snapshot="${table_name}_${today_yyyy_mm_dd}"

    psql $OPENALEX_DB -c "\
          create table ${table_snapshot} as (\
            select updated, ${json_field_name} \
            from ${table_name} \
            where merge_into_id is null and ${json_field_name} is not null \
          );"

    psql $OPENALEX_DB -c "create index on ${table_snapshot} (updated);"
    psql $OPENALEX_DB -c "analyze ${table_snapshot};"

    local updated_dates
    get_distinct_updated_dates $table_snapshot updated_dates

    for d in ${updated_dates[@]}
    do
        date_dir="${data_dir}/${entity_type}/updated_date=${d}"
        mkdir -p $date_dir
        echo "dumping ${entity_type} ${json_field_name} updated ${d} to ${date_dir}"

        part_file_prefix="$date_dir/part_"

        psql $OPENALEX_DB -c "\\copy ( \
          select ${json_field_name} from table_snapshot \
          where updated >= '$d'::date and updated < ('$d'::date + interval '1 day')::date \
        ) to stdout" |
        sed 's|\\\\|\\|' |
        split --numeric-suffixes --line-bytes=5GB --suffix-length=3 --filter='gzip > $FILE.gz' - $part_file_prefix
    done
}

export_table 'mid.json_concepts' 'concepts' 'json_save'
export_table 'mid.json_institutions' 'institutions' 'json_save'
export_table 'mid.json_venues' 'venues' 'json_save'
export_table 'mid.json_authors' 'authors' 'json_save'
export_table 'mid.json_works' 'works' 'json_save_with_abstract'

# make manifests
#for entity in venues institutions concepts
#do
#    ls $data_dir/$entity/*/* |
#    sed "s|$data_dir|s3://openalex/data|" |
#    jq -s -R 'split("\n") | map(select(length > 0))  | sort | map({url: .}) | {entries: .}' |
#    sed -z 's/\n\s*"url"/"url"/g' |
#    sed -z 's/\.gz"\s*\n\s*/.gz"/g' > $data_dir/$entity/manifest
#done
