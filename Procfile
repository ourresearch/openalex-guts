web: gunicorn views:app -w 2 --timeout 36000 --reload
run_queue_record: python -m scripts.queue --run --table=record --chunk=$QUEUE_WORKER_CHUNK_SIZE_RECORDS --name=queue_record-worker$DYNO-${i}
run_queue_work: python -m scripts.queue --run --table=work --method=process --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_record-worker$DYNO-${i} --randstart
run_queue_concept: python -m scripts.queue --run --table=concept --method=process --chunk=10
run_queue_concept_wiki: python -m scripts.queue --run --table=concept --method=save_wiki --chunk=100
run_queue_institution_wiki: python -m scripts.queue --run --table=institution --method=save_wiki --chunk=100
run_queue_new_concepts: python -m scripts.queue --run --table=work --method=new_work_concepts --chunk=$QUEUE_WORKER_CHUNK_SIZE --name=queue_record-worker$DYNO-${i}

