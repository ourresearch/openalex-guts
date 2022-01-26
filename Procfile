web: DATABASE_TO_USE=api gunicorn views:app -w 1 --timeout 36000 --reload

run_queue_refresh_work_a: python -m scripts.queue --run --table=work --method=refresh --chunk=100 --name=$DYNO
run_queue_refresh_work_b: python -m scripts.queue --run --table=work --method=refresh --chunk=100 --name=$DYNO
run_queue_refresh_work_c: python -m scripts.queue --run --table=work --method=refresh --chunk=100 --name=$DYNO
run_queue_refresh_work_d: python -m scripts.queue --run --table=work --method=refresh --chunk=100 --name=$DYNO

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

run_queue_store_work: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=work --method=store --chunk=1000 --name=queue_work$DYNO-${i} --randstart
run_queue_store_author: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=author --method=store --chunk=1000 --name=queue_author$DYNO-${i} --randstart
run_queue_store_venue: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=venue --method=store --chunk=100 --name=queue_venue$DYNO-${i} --randstart
run_queue_store_institution: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=institution --method=store --chunk=100 --name=queue_institution$DYNO-${i} --randstart
run_queue_store_concept: DATABASE_TO_USE=h1author python -m scripts.queue --run --table=concept --method=store --chunk=100 --name=queue_concept$DYNO-${i} --randstart

run_queue_record: python -m scripts.queue --run --table=record --method=process_record --chunk=100 --name=queue_record$DYNO-${i}

run_queue_work_concepts: python -m scripts.queue --run --table=work --method=new_work_concepts --chunk=500 --name=queue_work_concepts$DYNO-${i}
