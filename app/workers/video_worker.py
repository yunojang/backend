# app/worker/worker.py
from rq import Worker, Queue
from app.config.redis import get_redis


def main():
    listen = ["uploads"]  # 큐 이름
    connection = get_redis()
    queues = [Queue(name, connection=connection) for name in listen]
    Worker(queues, connection=connection).work(with_scheduler=True)


if __name__ == "__main__":
    main()
