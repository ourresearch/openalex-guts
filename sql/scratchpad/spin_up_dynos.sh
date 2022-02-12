aws redshift restore-from-cluster-snapshot --region us-east-1 \
    --snapshot-identifier openalex-4-2022-02-07-08-22-beforesavingupdatedworks --cluster-identifier openalex-h2author \
    --node-type ra3.16xlarge --number-of-nodes 8 \
    --publicly-accessible \
    --cluster-parameter-group-name experiment

aws redshift restore-from-cluster-snapshot --region us-east-1 \
    --snapshot-identifier openalex-4-2022-02-07-08-22-beforesavingupdatedworks --cluster-identifier openalex-q4work \
    --node-type ra3.16xlarge --number-of-nodes 8 \
    --publicly-accessible \
    --cluster-parameter-group-name experiment

heroku ps:scale run_queue_store_author_h1a=10:performance-l --remote heroku2
heroku ps:scale run_queue_store_author_h1b=10:performance-l --remote heroku2
heroku ps:scale run_queue_store_author_h1c=10:performance-l --remote heroku2
heroku ps:scale run_queue_store_author_h1d=10:performance-l --remote heroku2

heroku ps:scale run_queue_store_author_h2a=10:performance-l --remote heroku2
heroku ps:scale run_queue_store_author_h2b=10:performance-l --remote heroku2
heroku ps:scale run_queue_store_author_h2c=10:performance-l --remote heroku2
heroku ps:scale run_queue_store_author_h2d=10:performance-l --remote heroku2

heroku ps:scale run_queue_store_work_q1a=10:performance-l --remote heroku4
heroku ps:scale run_queue_store_work_q1b=10:performance-l --remote heroku4
heroku ps:scale run_queue_store_work_q1c=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q1d=10:performance-l --remote heroku3

heroku ps:scale run_queue_store_work_q2a=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q2b=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q2c=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q2d=10:performance-l --remote heroku3

heroku ps:scale run_queue_store_work_q3a=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q3b=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q3c=10:performance-l --remote heroku3
heroku ps:scale run_queue_store_work_q3d=10:performance-l --remote heroku3

heroku ps:scale run_queue_store_work_q4a=10:performance-l --remote heroku4
heroku ps:scale run_queue_store_work_q4b=10:performance-l --remote heroku4
heroku ps:scale run_queue_store_work_q4c=10:performance-l --remote heroku4
heroku ps:scale run_queue_store_work_q4d=10:performance-l --remote heroku4

