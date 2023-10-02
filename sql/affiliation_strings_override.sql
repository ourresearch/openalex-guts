/*
Run this query to insert rows into the queue.run_once_work_add_most_things table.
Start processing by turning on 10 dynos or so for the worker 'run_once_work_add_most_things' in
heroku via the openlaex-guts app. Rows will be removed from the queue table as they are processed.
*/

INSERT INTO queue.run_once_work_add_most_things(work_id, rand)
SELECT DISTINCT paper_id, random()
FROM mid.affiliation
JOIN mid.affiliation_string_v2
ON mid.affiliation.original_affiliation = mid.affiliation_string_v2.original_affiliation
WHERE mid.affiliation_string_v2.updated > '2023-09-14' ON CONFLICT DO NOTHING;