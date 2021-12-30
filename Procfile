web: gunicorn views:app -w 2 --timeout 36000 --reload

run_queue_store_work_1: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-a$DYNO-${i} --randstart
run_queue_store_work_2: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-b$DYNO-${i} --randstart
run_queue_store_work_3: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-c$DYNO-${i} --randstart
run_queue_store_work_4: DATABASE_TO_USE=4-LOW python -m scripts.queue --run --table=work --method=store --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-d$DYNO-${i} --randstart

run_queue_store_work_q1a: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q1a$DYNO-${i} --randstart
run_queue_store_work_q1b: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q1b$DYNO-${i} --randstart
run_queue_store_work_q1c: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q1c$DYNO-${i} --randstart
run_queue_store_work_q1d: DATABASE_TO_USE=q1work python -m scripts.queue --run --table=work --method=store_work_q1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q1d$DYNO-${i} --randstart

run_queue_store_work_q2a: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q2a$DYNO-${i} --randstart
run_queue_store_work_q2b: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q2b$DYNO-${i} --randstart
run_queue_store_work_q2c: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q2c$DYNO-${i} --randstart
run_queue_store_work_q2d: DATABASE_TO_USE=q2work python -m scripts.queue --run --table=work --method=store_work_q2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q2d$DYNO-${i} --randstart

run_queue_store_work_q3a: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q3a$DYNO-${i} --randstart
run_queue_store_work_q3b: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q3b$DYNO-${i} --randstart
run_queue_store_work_q3c: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q3c$DYNO-${i} --randstart
run_queue_store_work_q3d: DATABASE_TO_USE=q3work python -m scripts.queue --run --table=work --method=store_work_q3 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q3d$DYNO-${i} --randstart

run_queue_store_work_q4a: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q4a$DYNO-${i} --randstart
run_queue_store_work_q4b: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q4b$DYNO-${i} --randstart
run_queue_store_work_q4c: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q4c$DYNO-${i} --randstart
run_queue_store_work_q4d: DATABASE_TO_USE=q4work python -m scripts.queue --run --table=work --method=store_work_q4 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_work-q4d$DYNO-${i} --randstart

run_queue_store_author_h1a: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=author --method=store_author_h1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h1a$DYNO-${i} --randstart
run_queue_store_author_h1b: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=author --method=store_author_h1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h1b$DYNO-${i} --randstart
run_queue_store_author_h1c: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=author --method=store_author_h1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h1c$DYNO-${i} --randstart
run_queue_store_author_h1d: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=author --method=store_author_h1 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h1d$DYNO-${i} --randstart
run_queue_store_author_h2a:  DATABASE_TO_USE=h2author python -m scripts.queue --run --table=author --method=store_author_h2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h2a$DYNO-${i} --randstart
run_queue_store_author_h2b:  DATABASE_TO_USE=h2author python -m scripts.queue --run --table=author --method=store_author_h2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h2b$DYNO-${i} --randstart
run_queue_store_author_h2c:  DATABASE_TO_USE=h2author python -m scripts.queue --run --table=author --method=store_author_h2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h2c$DYNO-${i} --randstart
run_queue_store_author_h2d:  DATABASE_TO_USE=h2author python -m scripts.queue --run --table=author --method=store_author_h2 --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_author-h2d$DYNO-${i} --randstart

run_queue_store_venues: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=venue --method=store --chunk=100 --name=queue_venue-worker$DYNO-${i} --randstart
run_queue_store_institution: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=institution --method=store --chunk=100 --name=queue_institution-worker$DYNO-${i} --randstart
run_queue_store_concept: DATABASE_TO_USE=6-HIGH python -m scripts.queue --run --table=concept --method=store --chunk=100 --name=queue_concept-worker$DYNO-${i} --randstart
run_queue_record: python -m scripts.queue --run --table=record --chunk=$QUEUE_WORKER_CHUNK_SIZE_RECORDS --name=queue_record-worker$DYNO-${i}
