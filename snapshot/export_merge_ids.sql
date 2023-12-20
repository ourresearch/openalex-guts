create or replace procedure pg_temp.dump_merged_ids(tbl regclass, id_prefix text, entity_name text, id_column text) as
$$
declare
    distinct_merge_dates date[];
    merge_date date;
    bucket text := 'openalex-sandbox';
    data_prefix text := 'snapshot-merged-ids';
    csv_file_name text;
    additional_condition text := '';
    row_count int;
begin
    execute format('select array_agg(distinct merge_into_date::date) from %s where merge_into_id is not null', tbl) into distinct_merge_dates;

    if tbl::text = 'mid.author' then
        additional_condition := format(' and %s::bigint > 5000000000', id_column);
    end if;

    if distinct_merge_dates is not null then
        foreach merge_date in array distinct_merge_dates
        loop
            -- count the rows that would be exported
            execute format(
                'select count(*) from %s where merge_into_id is not null and merge_into_date::date = %L%s',
                tbl, merge_date, additional_condition
            ) into row_count;

            -- check if there are rows to export
            if row_count > 0 then
                csv_file_name = format('%s/merged_ids/%s/%s.csv', data_prefix, entity_name, merge_date);
                raise notice 'dumping merged % ids for % to s3://%/%', tbl, merge_date, bucket, csv_file_name;

                perform aws_s3.query_export_to_s3(
                    format(
                        'select merge_into_date::date as merge_date, %L || %s as id, %L || merge_into_id as merge_into_id from %s where merge_into_id is not null and merge_into_date::date = %L%s',
                        id_prefix, id_column, id_prefix, tbl, merge_date, additional_condition
                    ),
                    aws_commons.create_s3_uri(bucket, csv_file_name, 'us-east-1'),
                    options :='format csv, header'
                );
            else
                raise notice 'No rows to dump for % on %', tbl, merge_date;
            end if;
        end loop;
    else
        raise notice '% has no merged ids', tbl;
    end if;
end
$$
language 'plpgsql';


call pg_temp.dump_merged_ids('mid.work', 'W', 'works', 'paper_id');
call pg_temp.dump_merged_ids('mid.author', 'A', 'authors', 'author_id');
call pg_temp.dump_merged_ids('mid.journal', 'S', 'sources', 'journal_id');
call pg_temp.dump_merged_ids('mid.institution', 'I', 'institutions', 'affiliation_id');
call pg_temp.dump_merged_ids('mid.concept', 'C', 'concepts', 'field_of_study_id');
call pg_temp.dump_merged_ids('mid.funder', 'F', 'funders', 'funder_id');
call pg_temp.dump_merged_ids('mid.publisher', 'P', 'publishers', 'publisher_id');
