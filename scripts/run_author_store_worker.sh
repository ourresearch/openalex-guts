for (( i=1; i<=$AUTHOR_STORE_WORKERS_PER_DYNO; i++ ))
do
  COMMAND="python -m scripts.fast_queue --entity=author --method=store --chunk=$AUTHOR_STORE_CHUNK_SIZE"
  echo $COMMAND
  $COMMAND&
done
trap "kill 0" INT TERM EXIT
wait
