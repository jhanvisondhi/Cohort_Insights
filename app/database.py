import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from pymongo import AsyncMongoClient
from redis.asyncio import Redis


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    mongo_uri = os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")
    mongo_client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        mongo_uri, serverSelectionTimeoutMS=2000
    )
    app.state.mongo_client = mongo_client
    try:
        redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
        redis_client = Redis.from_url(
            redis_url, socket_connect_timeout=2, socket_timeout=2
        )
        app.state.redis_client = redis_client
        try:
            yield
        finally:
            await redis_client.aclose()
    finally:
        await mongo_client.close()


def get_mongo_client(request: Request) -> AsyncMongoClient[dict[str, Any]]:
    return request.app.state.mongo_client


def get_redis_client(request: Request) -> Redis:
    return request.app.state.redis_client
