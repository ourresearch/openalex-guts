import argparse
import csv
from time import gmtime, mktime, sleep

from redis import Redis

from app import REDIS_QUEUE_URL
from models import REDIS_WORK_QUEUE

_redis = Redis.from_url(REDIS_QUEUE_URL)


def front_of_fast_queue_one_batch(work_ids, no_redis=False):
    """Adds a list of work IDs to the redis fast queue as one batch.
    `work_ids` is a list of integers
    `if `no_redis` is True: return batch but don't actually make the change to the fast queue
    Returns the batch (dict used to add to redis queue)"""
    batch = {work_id: -1 for work_id in work_ids}
    if (batch) and (no_redis is False):
        _redis.zadd(REDIS_WORK_QUEUE, batch)
    return batch

def front_of_fast_queue(file_name, batch_size, no_redis=False):
    """
    Takes a list of work IDs in a CSV and moves them to the front of the fast queue.
    Run with CSV file in root of project directory.
    Test and see IDs: `heroku local:run python -m scripts.front_of_fast_queue --file ids_to_run.csv --no-redis`
    Store in redis: `heroku local:run python -m scripts.front_of_fast_queue --file ids_to_run.csv`
    """

    with open(file_name, 'r') as f:
        count = 0

        reader = csv.reader(f)
        work_ids_for_fast_queue = []

        # iterate through the CSV file in batches
        for i, row in enumerate(reader):
            if i % batch_size == 0 and work_ids_for_fast_queue:
                batch = front_of_fast_queue_one_batch(work_ids_for_fast_queue, no_redis=no_redis)
                if no_redis:
                    print(batch)
                work_ids_for_fast_queue = []
                print(f"processed {count} rows")
                sleep(0.5)

            work_id = int(row[0].replace("https://openalex.org/W", ""))
            work_ids_for_fast_queue.append(work_id)
            count += 1

        # add remaining items in the batch if any
        if work_ids_for_fast_queue:
            batch = front_of_fast_queue_one_batch(work_ids_for_fast_queue, no_redis=no_redis)
            if no_redis:
                print(batch)
            print(f"finished processing file, count is {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Front of Fast Queue Script")
    parser.add_argument("--file", required=True, help="Input CSV file name")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size (default: 10000)")
    parser.add_argument("--no-redis", action="store_true", help="Test script without saving to Redis")
    args = parser.parse_args()

    front_of_fast_queue(args.file, args.batch_size, args.no_redis)
