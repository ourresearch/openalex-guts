for (( i=1; i<=$PROCESS_SDG_WORKERS_PER_DYNO; i++ ))
do
  COMMAND="python -m scripts.queue_work_process_sdgs --chunk=100"
  echo $COMMAND
  $COMMAND&
done
trap "kill 0" INT TERM EXIT
wait
