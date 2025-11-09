# app/adapters/redis.py
from redis import Redis
from .env import settings


def get_redis() -> Redis:
    return Redis.from_url(settings.REDIS_URL)
