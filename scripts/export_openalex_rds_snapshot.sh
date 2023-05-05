#!/bin/bash

# 1. back up current snapshot
#   $ aws s3 sync s3://openalex s3://openalex-sandbox/snapshot-backups/openalex-jsonl/current-date-yyyy-mm-dd

# 2. run this script to creates the new contents of s3://openalex/data/ in a local temp directory ${data_dir}
#   $ bash ./scripts/export_openalex_rds_snapshot.sh
#   "dumping entity rows to local data dir ${data_dir}"

# 3. export merged ids
#   # 3.1 remove old ones
#   $ aws s3 rm s3://openalex-sandbox/snapshot-merged-ids/merged_ids --recursive

#   # 3.2 export the new list
#   psql $OPENALEX_DB -f ./sql/export_merge_ids.sql

#   # 3.3 copy the result to your local snapshot, gzip the files
#   aws s3 sync s3://openalex-sandbox/snapshot-merged-ids/merged_ids ${data_dir}/merged_ids
#   gzip ${data_dir}/merged_ids/*/*.csv
#
#   # 3.4 copy files to s3 staging folder
#   aws s3 cp ${data_dir}/merged_ids s3://openalex-sandbox/snapshot-yyyy-mm-dd-staging/data/merged_ids/ --recursive

# 4. make manifests
#   included in script, test on next run of snapshot

# 5. add txt files and browse page
#   date the current release notes
#   in files-for-datadumps/standard-format/RELEASE_NOTES.txt, change "Next Release" to "RELEASE YYYY-MM-DD"
#   $ git add files-for-datadumps/RELEASE_NOTES.txt
#   $ git commit -m "added YYYY-MM-DD release notes"
#
# 5. upload to S3 for QA
#   aws s3 sync ${data_dir}/..  s3://openalex-sandbox/snapshot-yyyy-mm-dd-staging
#
# 6. upload approved copy to s3
#   set credentials for s3://openalex (separate from s3://openalex-sandbox)
#   delete existing files: aws s3 rm --recursive s3://openalex/data/
#   browse to data folder (ex /tmp/tmp.AsqlHWZ3U0/), and run: aws s3 sync . s3://openalex
#   browse to files-for-datadumps/standard-format and run: aws s3 cp RELEASE_NOTES.txt s3://openalex/RELEASE_NOTES.txt
#   check result at: https://openalex.s3.amazonaws.com/browse.html

data_dir=$(mktemp -d)/data
today_yyyy_mm_dd=$(date +%Y_%m_%d)

echo "dumping entity rows to local data dir ${data_dir}"

get_distinct_changed_dates() {
    table_name=$1
    local -n dates=$2

    echo "get distinct changed dates for ${table_name}"

    dates=( $(
        psql $OPENALEX_DB -q -t  -c "select distinct changed_date from ${table_name}"
    ) )
}

export_table() {
    table_name=$1
    entity_type=$2
    json_field_name=$3

    table_snapshot="${table_name}_${today_yyyy_mm_dd}"

    echo "creating ${table_snapshot} - this will take a while"

    psql $OPENALEX_DB -c "\
          create table if not exists ${table_snapshot} as (\
            select changed::date as changed_date, ${json_field_name} \
            from ${table_name} \
            where merge_into_id is null and ${json_field_name} is not null \
          );"

    psql $OPENALEX_DB -c "create index on ${table_snapshot} (changed_date);"
    psql $OPENALEX_DB -c "analyze ${table_snapshot};"

    local changed_dates
    get_distinct_changed_dates $table_snapshot changed_dates

    for d in ${changed_dates[@]}
    do
        date_dir="${data_dir}/${entity_type}/updated_date=${d}"
        mkdir -p $date_dir
        echo "dumping ${entity_type} ${json_field_name} updated ${d} to ${date_dir}"

        part_file_prefix="$date_dir/part_"

        psql $OPENALEX_DB -c "\\copy ( \
          select ${json_field_name} from ${table_snapshot} \
          where changed_date = '$d' \
        ) to stdout" |
        sed 's|\\\\|\\|g' |
        split --numeric-suffixes --line-bytes=5GB --suffix-length=3 --filter='gzip > $FILE.gz' - $part_file_prefix

    done
}

make_manifests() {
    remote_data_dir='s3://openalex/data'
    for entity_type in concepts institutions sources publishers funders authors works
    do
        let total_content_length=0
        let total_record_count=0

        entity_dir="${data_dir}/${entity_type}"
        manifest="${entity_dir}/manifest"
        echo $manifest
        echo -e "{\n  \"entries\": [" > $manifest

        for f in ${entity_dir}/updated_date=*/*.gz
        do
            echo $f
            s3_url=$(echo $f | sed "s|${data_dir}|${remote_data_dir}|")
            content_length=$(wc -c $f | cut -d ' ' -f 1)
            record_count=$(unpigz -c $f | wc -l)

            let total_content_length+=$content_length
            let total_record_count+=$record_count

            echo "    {\"url\": \"${s3_url}\", \"meta\": { \"content_length\": ${content_length}, \"record_count\": ${record_count} }}," >> $manifest
        done

        # remove trailing comma
        truncate -s -2 $manifest
        echo -n -e "\n" >> $manifest

        echo "  ]," >> $manifest
        echo "  \"meta\": {" >> $manifest
        echo "    \"content_length\": $total_content_length," >> $manifest
        echo "    \"record_count\": $total_record_count" >> $manifest
        echo "  }" >> $manifest
        echo "}" >> $manifest
    done
}

export_table 'mid.json_concepts' 'concepts' 'json_save'
export_table 'mid.json_funders' 'funders' 'json_save'
export_table 'mid.json_institutions' 'institutions' 'json_save'
export_table 'mid.json_publishers' 'publishers' 'json_save'
export_table 'mid.json_sources' 'sources' 'json_save'
export_table 'mid.json_authors' 'authors' 'json_save'
export_table 'mid.json_works' 'works' 'json_save_with_abstract'

echo "creating manifests"

make_manifests