web: DATABASE_TO_USE=api gunicorn views:app -w 1 --timeout 36000 --reload

#run_queue_record: python -m scripts.queue --run --table=record --method=process_record --chunk=$RT_RECORD_CHUNK_SIZE --name=queue_record$DYNO
run_queue_record: python -m scripts.queue_record_assign_work --chunk=$RT_RECORD_CHUNK_SIZE
# run_queue_work_add_everything: python -m scripts.queue --run --table=work --method=add_everything --chunk=10 --name=work_add_everything$DYNO
run_once_work_add_everything: python -m scripts.queue_work_add_everything --chunk=$WORK_ADD_EVERYTHING_CHUNK_SIZE
run_once_work_add_most_things: python -m scripts.queue_work_add_everything --partial --chunk=$WORK_ADD_EVERYTHING_CHUNK_SIZE
run_once_work_update_authors: python -m scripts.queue_work_update_authors --chunk=$WORK_ADD_EVERYTHING_CHUNK_SIZE

fast_store_authors: python -m scripts.fast_queue --entity=author --method=store --chunk=$AUTHOR_STORE_CHUNK_SIZE
fast_store_works: python -m scripts.fast_queue --entity=work --method=store --chunk=$WORK_STORE_CHUNK_SIZE
fast_store_concepts: python -m scripts.fast_queue --entity=concept --method=store --chunk=100
fast_store_sources: python -m scripts.fast_queue --entity=source --method=store --chunk=1
fast_store_institutions: python -m scripts.fast_queue --entity=institution --method=store --chunk=10
fast_store_publishers: python -m scripts.fast_queue --entity=publisher --method=store --chunk=1
fast_store_funders: python -m scripts.fast_queue --entity=funder --method=store --chunk=1

fast_update_once_update_institutions: python -m scripts.fast_queue --entity=work --method=update_once_update_institutions --chunk=100
fast_update_once_add_work_concepts: python -m scripts.fast_queue --entity=work --method=update_once_add_work_concepts --chunk=100
fast_update_once_add_related_works: python -m scripts.fast_queue --entity=work --method=update_once_add_related_works --chunk=100
