import time
import logging
from redis import Redis
import csv
from app import REDIS_QUEUE_URL
from models import REDIS_WORK_QUEUE

logger = logging.getLogger(__name__)

def add_missing_ids_to_redis(batch_size=100):
    _redis = Redis.from_url(REDIS_QUEUE_URL)

    with open('scripts/work_ids_2024_10_17.csv', 'r') as f:
        reader = csv.reader(f)
        paper_ids = [row[0] for row in reader]

    redis_queue_time = time.time()

    for i in range(0, len(paper_ids), batch_size):
        batch = paper_ids[i:i + batch_size]
        missing_ids = {}

        for paper_id in batch:
            if not _redis.zscore(REDIS_WORK_QUEUE, paper_id):
                print(f"Adding missing ID {paper_id} to Redis queue.")
                missing_ids[paper_id] = redis_queue_time

        if missing_ids:
            _redis.zadd(REDIS_WORK_QUEUE, missing_ids)
            logger.info(f"Added {len(missing_ids)} missing paper IDs to Redis queue.")

        logger.info(f"Processed batch {i // batch_size + 1} of {len(paper_ids) // batch_size + 1}")

    logger.info("Finished adding missing IDs to Redis queue.")

if __name__ == "__main__":
    add_missing_ids_to_redis()
