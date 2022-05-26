web: DATABASE_TO_USE=api gunicorn views:app -w 1 --timeout 36000 --reload

run_queue_record: python -m scripts.queue --run --table=record --method=process_record --chunk=10 --name=queue_record$DYNO

run_queue_work_add_everything: python -m scripts.queue --run --table=work --method=add_everything --chunk=10 --name=$DYNO --randstart

run_queue_store_work: python -m scripts.queue --run --table=work --method=store --chunk=1000 --name=queue_work$DYNO --randstart
run_queue_store_author: python -m scripts.queue --run --table=author --method=store --chunk=1000 --name=queue_author$DYNO --randstart
run_queue_store_venue: python -m scripts.queue --run --table=venue --method=store --chunk=10 --name=queue_venue$DYNO --randstart
run_queue_store_institution: python -m scripts.queue --run --table=institution --method=store --chunk=10 --name=queue_institution$DYNO --randstart
run_queue_store_concept: python -m scripts.queue --run --table=concept --method=store --chunk=1000 --name=queue_concept$DYNO --randstart

run_queue_work_add_related_works: python -m scripts.queue --run --table=work --method=add_related_works --chunk=100 --name=$DYNO
run_queue_work_add_concepts: python -m scripts.queue --run --table=work --method=new_work_concepts --chunk=10 --name=$DYNO

fast_add_everything: bash scripts/run_add_everything_worker.sh
fast_store_authors: bash scripts/run_author_store_worker.sh
fast_store_venues: python -m scripts.fast_queue --entity=venue --method=store --chunk=10
fast_update_institutions: python -m scripts.fast_queue --entity=work --method=update_institutions --chunk=100
fast_update_institutions: python -m scripts.fast_queue --entity=work --method=add_references --chunk=100