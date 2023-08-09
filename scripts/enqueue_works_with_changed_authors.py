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
        redis_queue_mapping = {paper_id: mktime(gmtime(0)) for paper_id in paper_ids}
        logger.info(f'queueing {len(redis_queue_mapping)} works')
        _redis.zadd(REDIS_WORK_QUEUE, redis_queue_mapping)
        WorkToEnqueue.query.delete()
        db.session.commit()
    else:
        logger.info('no works to queue')
