web: gunicorn views:app -w 2 --timeout 36000 --reload

run_queue_store_work_1: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-a$DYNO-${i} --randstart
run_queue_store_work_2: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-b$DYNO-${i} --randstart
run_queue_store_work_3: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-c$DYNO-${i} --randstart
run_queue_store_work_4: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-d$DYNO-${i} --randstart

run_queue_store_work_q1a: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q1a$DYNO-${i} --randstart
run_queue_store_work_q1b: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q1b$DYNO-${i} --randstart
run_queue_store_work_q1c: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q1c$DYNO-${i} --randstart
run_queue_store_work_q1d: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q1d$DYNO-${i} --randstart

run_queue_store_work_q2a: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q2a$DYNO-${i} --randstart
run_queue_store_work_q2b: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q2b$DYNO-${i} --randstart
run_queue_store_work_q2c: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q2c$DYNO-${i} --randstart
run_queue_store_work_q2d: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q2d$DYNO-${i} --randstart

run_queue_store_work_q3a: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q3a$DYNO-${i} --randstart
run_queue_store_work_q3b: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q3b$DYNO-${i} --randstart
run_queue_store_work_q3c: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q3c$DYNO-${i} --randstart
run_queue_store_work_q3d: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q3d$DYNO-${i} --randstart

run_queue_store_work_q4a: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q4a$DYNO-${i} --randstart
run_queue_store_work_q4b: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q4b$DYNO-${i} --randstart
run_queue_store_work_q4c: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q4c$DYNO-${i} --randstart
run_queue_store_work_q4d: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-worker-q4d$DYNO-${i} --randstart

run_queue_store_author_a: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=author --method=store_author_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-author-a$DYNO-${i} --randstart
run_queue_store_author_b: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=author --method=store_author_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-author-b$DYNO-${i} --randstart
run_queue_store_author_c: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=author --method=store_author_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-author-c$DYNO-${i} --randstart
run_queue_store_author_d: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=author --method=store_author_low --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-author-d$DYNO-${i} --randstart
run_queue_store_author_w:  DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=author --method=store_author_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-worker-w$DYNO-${i} --randstart
run_queue_store_author_x:  DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=author --method=store_author_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-worker-x$DYNO-${i} --randstart
run_queue_store_author_y:  DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=author --method=store_author_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-worker-y$DYNO-${i} --randstart
run_queue_store_author_z:  DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=author --method=store_author_high --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-worker-z$DYNO-${i} --randstart

run_queue_store_venues: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=venue --method=store --chunk=100 --name=queue_venue-worker$DYNO-${i} --randstart
run_queue_store_institution: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=institution --method=store --chunk=100 --name=queue_institution-worker$DYNO-${i} --randstart
run_queue_store_concept: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=concept --method=store --chunk=100 --name=queue_concept-worker$DYNO-${i} --randstart
run_queue_record: python -m scripts.queue --run --table=record --chunk=$QUEUE_WORKER_CHUNK_SIZE_RECORDS --name=queue_record-worker$DYNO-${i}

