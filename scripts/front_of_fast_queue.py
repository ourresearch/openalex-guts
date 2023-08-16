import csv
import argparse
from time import gmtime, mktime, sleep

from redis import Redis

from app import REDIS_QUEUE_URL
from scripts.fast_queue import REDIS_WORK_QUEUE


_redis = Redis.from_url(REDIS_QUEUE_URL)


def front_of_fast_queue(file_name, batch_size, no_redis=False):
    """
    Takes a list of work IDs in a CSV and moves them to the front of the fast queue.
    Run with CSV file in root of project directory.
    Test and see IDs: `heroku local:run python -m scripts.front_of_fast_queue --file ids_to_run.csv --no-redis`
    Store in redis: `heroku local:run python -m scripts.front_of_fast_queue --file ids_to_run.csv`
    """

    with open(file_name, 'r') as f:
        count = 0
        epoch_time_seconds = mktime(gmtime(0))  # "front" of queue (oldest)

        reader = csv.reader(f)
        batch = {}

        # iterate through the CSV file in batches
        for i, row in enumerate(reader):
            if i % batch_size == 0 and batch:
                if no_redis:
                    print(batch)
                else:
                    _redis.zadd(REDIS_WORK_QUEUE, batch)
                batch = {}
                print(f"processed {count} rows")
                sleep(0.5)

            work_id = int(row[0].replace("https://openalex.org/W", ""))
            batch[work_id] = epoch_time_seconds
            count += 1

        # add remaining items in the batch if any
        if batch:
            if no_redis:
                print(batch)
            else:
                _redis.zadd(REDIS_WORK_QUEUE, batch)
            print(f"finished processing file, count is {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Front of Fast Queue Script")
    parser.add_argument("--file", required=True, help="Input CSV file name")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size (default: 1000)")
    parser.add_argument("--no-redis", action="store_true", help="Test script without saving to Redis")
    args = parser.parse_args()

    front_of_fast_queue(args.file, args.batch_size, args.no_redis)
