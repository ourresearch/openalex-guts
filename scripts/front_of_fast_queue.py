import csv
from time import gmtime, mktime, sleep

from redis import Redis

from app import REDIS_QUEUE_URL
from scripts.fast_queue import REDIS_WORK_QUEUE


_redis = Redis.from_url(REDIS_QUEUE_URL)


def front_of_fast_queue(file_name):
    """
    Takes a list of work IDs in a CSV and moves them to the front of the fast queue.
    Run with `heroku local:run python -m scripts.front_of_fast_queue`, with the CSV file in the root directory.
    """

    with open(file_name, 'r') as f:
        count = 0
        batch_size = 1000
        epoch_time_seconds = mktime(gmtime(0))  # "front" of queue (oldest)

        reader = csv.reader(f)
        batch = {}

        # iterate through the CSV file in batches
        for i, row in enumerate(reader):
            if i % batch_size == 0 and batch:
                _redis.zadd(REDIS_WORK_QUEUE, batch)
                print(batch)
                batch = {}
                print(f"processed {count} rows")
                sleep(0.5)

            work_id = row[0]
            batch[work_id] = epoch_time_seconds
            count += 1

        # add remaining items in the batch if any
        if batch:
            _redis.zadd(REDIS_WORK_QUEUE, batch)
            print(f"finished processing file, count is {count}")


if __name__ == "__main__":
    front_of_fast_queue('run_once_work_add_most_things.csv')
