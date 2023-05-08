import random
from requests_cache import CachedSession, RedisCache
from redis import Redis

from app import REDIS_URL

connection = Redis.from_url(REDIS_URL)


def cached_session():
    cache_backend = RedisCache(connection=connection, expire_after=None)
    random_expire_one_to_three_days = random.randint(86400, 259200)
    session = CachedSession(cache_name="cache", backend=cache_backend, expire_after=random_expire_one_to_three_days)
    return session
