import os
import time

from aws_lambda_powertools.utilities.idempotency import (
    RedisCachePersistenceLayer,
    idempotent,
)

REDIS_HOST = os.getenv("RedisEndpoint", "")
persistence_layer = RedisCachePersistenceLayer(host=REDIS_HOST, port=6379, ssl=True)


@idempotent(persistence_store=persistence_layer)
def lambda_handler(event, context):
    time.sleep(5)

    return event
