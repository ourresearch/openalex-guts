for (( i=1; i<=$UPSERT_MAG_RECORDS_WORKERS_PER_DYNO; i++ ))
do
  COMMAND="python upsert_mag_records.py"
  echo $COMMAND
  $COMMAND&
done
trap "kill 0" INT TERM EXIT
wait
