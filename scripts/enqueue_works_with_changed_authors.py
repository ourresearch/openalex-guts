from time import mktime, gmtime

from redis import Redis

from app import REDIS_QUEUE_URL
from app import db
from app import logger
from scripts.fast_queue import REDIS_WORK_QUEUE

_redis = Redis.from_url(REDIS_QUEUE_URL)


class WorkToEnqueue(db.Model):
    __table_args__ = {'schema': 'authorships'}
    __tablename__ = "works_to_enqueue"

    paper_id = db.Column(db.BigInteger, primary_key=True)


if __name__ == "__main__":
    paper_ids = [r[0] for r in db.session.query(WorkToEnqueue.paper_id).all()]
    if paper_ids:
        num_paper_ids = len(paper_ids)
        logger.info(f'got {num_paper_ids} works to queue')
        queue_chunk_size = 100000

        for i in range(0, num_paper_ids, queue_chunk_size):
            redis_queue_mapping = {paper_id: mktime(gmtime(0)) for paper_id in paper_ids[i:i+queue_chunk_size]}
            logger.info(f'queueing works {i} - {i+queue_chunk_size}')
            _redis.zadd(REDIS_WORK_QUEUE, redis_queue_mapping)

        WorkToEnqueue.query.delete()
        db.session.commit()
    else:
        logger.info('no works to queue')
