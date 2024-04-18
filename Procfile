web: gunicorn views:app

#run_queue_record: python -m scripts.queue --run --table=record --method=process_record --chunk=$RT_RECORD_CHUNK_SIZE --name=queue_record$DYNO
run_queue_record: python -m scripts.queue_record_assign_work --chunk=$RT_RECORD_CHUNK_SIZE
# run_queue_work_add_everything: python -m scripts.queue --run --table=work --method=add_everything --chunk=10 --name=work_add_everything$DYNO
run_once_work_add_everything: python -m scripts.queue_work_add_everything --chunk=$WORK_ADD_EVERYTHING_CHUNK_SIZE
run_once_work_add_most_things: python -m scripts.queue_work_add_everything --partial --chunk=$WORK_ADD_MOST_THINGS_CHUNK_SIZE
run_once_work_add_some_things: python -m scripts.add_things_queue
run_once_work_update_authors: python -m scripts.queue_work_update_authors --chunk=$WORK_ADD_EVERYTHING_CHUNK_SIZE
run_once_work_process_embeddings_bash: bash scripts/run_process_embeddings_worker.sh
run_once_work_store_vectors_bash: bash scripts/run_store_vectors_worker.sh
run_once_work_process_sdgs: python -m scripts.queue_work_process_sdgs --chunk=100
run_once_work_process_sdgs_bash: bash scripts/run_process_sdg_worker.sh

fast_store_authors: python -m scripts.fast_queue --entity=author --method=store --chunk=$AUTHOR_STORE_CHUNK_SIZE
fast_store_works: python -m scripts.fast_queue --entity=work --method=store --chunk=$WORK_STORE_CHUNK_SIZE
fast_store_concepts: python -m scripts.fast_queue --entity=concept --method=store --chunk=100
fast_store_sources: python -m scripts.fast_queue --entity=source --method=store --chunk=1
fast_store_institutions: python -m scripts.fast_queue --entity=institution --method=store --chunk=10
fast_store_publishers: python -m scripts.fast_queue --entity=publisher --method=store --chunk=1
fast_store_funders: python -m scripts.fast_queue --entity=funder --method=store --chunk=1
fast_store_topics: python -m scripts.fast_queue --entity=topic --method=store --chunk=1
fast_store_domains: python -m scripts.fast_queue --entity=domain --method=store --chunk=1
fast_store_fields: python -m scripts.fast_queue --entity=field --method=store --chunk=1
fast_store_subfields: python -m scripts.fast_queue --entity=subfield --method=store --chunk=1
fast_store_sdgs: python -m scripts.fast_queue --entity=sdg --method=store --chunk=1
fast_store_keywords: python -m scripts.fast_queue --entity=keyword --method=store --chunk=1
fast_store_countries: python -m scripts.fast_queue --entity=country --method=store --chunk=1
fast_store_continents: python -m scripts.fast_queue --entity=continent --method=store --chunk=1
fast_store_languages: python -m scripts.fast_queue --entity=language --method=store --chunk=1
fast_store_institution_types: python -m scripts.fast_queue --entity=institution_type --method=store --chunk=1
fast_store_source_types: python -m scripts.fast_queue --entity=source_type --method=store --chunk=1
fast_store_work_types: python -m scripts.fast_queue --entity=work_type --method=store --chunk=1
fast_store_license: python -m scripts.fast_queue --entity=license --method=store --chunk=1

fast_update_once_update_institutions: python -m scripts.fast_queue --entity=work --method=update_once_update_institutions --chunk=100
fast_update_once_add_work_concepts: python -m scripts.fast_queue --entity=work --method=update_once_add_work_concepts --chunk=100
fast_update_once_add_related_works: python -m scripts.fast_queue --entity=work --method=update_once_add_related_works --chunk=100

run_once_make_parseland_records: python -m scripts.queue_make_parseland_rt_records --chunk=$PARSELAND_RT_CHUNK_SIZE
