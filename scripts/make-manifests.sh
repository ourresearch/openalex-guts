# target format:
#
#    {
#  "entries": [
#    {"url":"s3://openalex/data/authors/updated_date=2022-04-14/0000_part_00.gz", "meta": { "content_length": 615393307, "record_count": 1819737 }},
#    {"url":"s3://openalex/data/authors/updated_date=2022-04-15/0000_part_00.gz", "meta": { "content_length": 1528786937, "record_count": 4673908 }},
#    {"url":"s3://openalex/data/authors/updated_date=2022-04-28/0000_part_00.gz", "meta": { "content_length": 147885976, "record_count": 273711 }},
#    {"url":"s3://openalex/data/authors/updated_date=2022-04-29/0000_part_00.gz", "meta": { "content_length": 787794, "record_count": 2216 }},
#    {"url":"s3://openalex/data/authors/updated_date=2022-04-30/0000_part_00.gz", "meta": { "content_length": 296647, "record_count": 789 }},
#    {"url":"s3://openalex/data/authors/updated_date=2022-05-01/0000_part_00.gz", "meta": { "content_length": 1258077, "record_count": 3290 }},

LOCAL_DATA_DIR='/home/ubuntu/misc/openalex-json-qa-2022-07-09/data'
REMOTE_DATA_DIR='s3://openalex/data'

for entity_type in concepts institutions sources authors works publishers
do
    let total_content_length=0
    let total_record_count=0

    entity_dir="${LOCAL_DATA_DIR}/${entity_type}"
    manifest="${entity_dir}/manifest"
    echo $manifest
    echo -e "{\n  \"entries\": [" > $manifest

    for f in ${entity_dir}/updated_date=*/*.gz
    do
        echo $f
        s3_url=$(echo $f | sed "s|${LOCAL_DATA_DIR}|${REMOTE_DATA_DIR}|")
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



