for (( i=1; i<=$PROCESS_EMBEDDINGS_WORKERS_PER_DYNO; i++ ))
do
  COMMAND="python -m scripts.queue_work_store_vectors --chunk=100"
  echo $COMMAND
  $COMMAND&
done
trap "kill 0" INT TERM EXIT
wait
