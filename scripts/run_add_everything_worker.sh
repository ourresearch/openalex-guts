for (( i=1; i<=$WORK_ADD_EVERYTHING_WORKERS_PER_DYNO; i++ ))
do
  COMMAND="python -m scripts.fast_queue --entity=work --method=add_everything --chunk=$WORK_ADD_EVERYTHING_CHUNK_SIZE"
  echo $COMMAND
  $COMMAND&
done
trap "kill 0" INT TERM EXIT
wait
