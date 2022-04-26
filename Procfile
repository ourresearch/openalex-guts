web: DATABASE_TO_USE=api gunicorn views:app -w 1 --timeout 36000 --reload

run_queue_record: python -m scripts.queue --run --table=record --method=process_record --chunk=100 --name=queue_record$DYNO-${i}

run_queue_work_add_everything: python -m scripts.queue --run --table=work --method=add_everything --chunk=100 --name=$DYNO --randstart

run_queue_store_work: python -m scripts.queue --run --table=work --method=store --chunk=1000 --name=queue_work$DYNO-${i} --randstart
run_queue_store_author: python -m scripts.queue --run --table=author --method=store --chunk=1000 --name=queue_author$DYNO-${i} --randstart
run_queue_store_venue: python -m scripts.queue --run --table=venue --method=store --chunk=10 --name=queue_venue$DYNO-${i} --randstart
run_queue_store_institution: python -m scripts.queue --run --table=institution --method=store --chunk=10 --name=queue_institution$DYNO-${i} --randstart
run_queue_store_concept: python -m scripts.queue --run --table=concept --method=store --chunk=1000 --name=queue_concept$DYNO-${i} --randstart

run_queue_work_add_related_works: python -m scripts.queue --run --table=work --method=add_related_works --chunk=100 --name=$DYNO
run_queue_work_add_concepts: python -m scripts.queue --run --table=work --method=new_work_concepts --chunk=10 --name=$DYNO
