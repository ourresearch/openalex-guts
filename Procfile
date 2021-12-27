web: gunicorn views:app -w 2 --timeout 36000 --reload
run_queue_store_work_a: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store_work_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-a$DYNO-${i} --randstart
run_queue_store_work_b: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store_work_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-b$DYNO-${i} --randstart
run_queue_store_work_c: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store_work_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-c$DYNO-${i} --randstart
run_queue_store_work_d: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store_work_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-d$DYNO-${i} --randstart
run_queue_store_work_w: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=work --method=store_work_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-w$DYNO-${i} --randstart
run_queue_store_work_x: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=work --method=store_work_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-x$DYNO-${i} --randstart
run_queue_store_work_y: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=work --method=store_work_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-y$DYNO-${i} --randstart
run_queue_store_work_z: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=work --method=store_work_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-z$DYNO-${i} --randstart
run_queue_store_author: python -m scripts.queue --run --table=author --method=store --chunk=10 --name=queue_author-worker$DYNO-${i} --randstart
run_queue_store_venues: python -m scripts.queue --run --table=venue --method=store --chunk=10 --name=queue_venue-worker$DYNO-${i} --randstart
run_queue_store_institution: python -m scripts.queue --run --table=institution --method=store --chunk=10 --name=queue_institution-worker$DYNO-${i} --randstart
run_queue_store_concept: python -m scripts.queue --run --table=concept --method=store --chunk=10 --name=queue_concept-worker$DYNO-${i} --randstart
run_queue_record: python -m scripts.queue --run --table=record --chunk=$QUEUE_WORKER_CHUNK_SIZE_RECORDS --name=queue_record-worker$DYNO-${i}
run_queue_concept_wiki: python -m scripts.queue --run --table=concept --method=save_wiki --chunk=100
run_queue_institution_wiki: python -m scripts.queue --run --table=institution --method=save_wiki --chunk=100
run_queue_new_concepts: python -m scripts.queue --run --table=work --method=new_work_concepts --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_record-worker$DYNO-${i}
run_queue_concepts_clean_metadata: python -m scripts.queue --run --table=concept --method=clean_metadata --chunk=100 --name=queue_record-worker$DYNO-${i}

