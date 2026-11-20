import asyncio
import json
import logging
import os
import random
import re
import uuid
from asyncio import sleep
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING, AsyncMongoClient, ReturnDocument
from pymongo.errors import PyMongoError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.services.documents import CACHE_SECONDS, DocumentService, cache_key

logger = logging.getLogger(__name__)


async def process_one(service: DocumentService) -> bool:
    now = datetime.now(timezone.utc)
    lease_id = str(uuid.uuid4())
    document = await service.collection.find_one_and_update(
        {
            "status": {"$in": ["queued", "processing", "enriching"]},
            "$or": [
                {"lease_until": {"$exists": False}},
                {"lease_until": {"$lt": now}},
            ],
        },
        {"$set": {"lease_id": lease_id, "lease_until": now + timedelta(seconds=120)}},
        sort=[("created_at", ASCENDING), ("_id", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if document is None:
        return False

    job_filter = {
        "_id": document["_id"],
        "version": document["version"],
        "lease_id": lease_id,
    }
    if document["status"] in ("queued", "processing"):
        update_result = await service.collection.update_one(
            job_filter, {"$set": {"status": "processing", "stages.processing": "running"}}
        )
        if not update_result.matched_count:
            return True
        await sleep(random.uniform(10, 20))
        if random.random() < 0.1:
            await fail_stage(service, document, job_filter, "processing")
            return True
        summary = " ".join(document["content"].split()[:30])
        update_result = await service.collection.update_one(
            job_filter,
            {
                "$set": {
                    "status": "enriching",
                    "stages.processing": "completed",
                    "stages.enriching": "running",
                    "result": {
                        "version": document["version"],
                        "summary": summary,
                        "tags": None,
                    },
                }
            },
        )
        if not update_result.modified_count:
            return True
    else:
        summary = document["result"]["summary"]
        update_result = await service.collection.update_one(
            job_filter, {"$set": {"stages.enriching": "running"}}
        )
        if not update_result.matched_count:
            return True

    await sleep(random.uniform(5, 15))
    if random.random() < 0.1:
        await fail_stage(service, document, job_filter, "enriching")
        return True
    tags = list(dict.fromkeys(re.findall(r"[a-z0-9]+", summary.lower())))[:5]
    update_result = await service.collection.update_one(
        job_filter,
        {
            "$set": {
                "status": "completed",
                "stages.enriching": "completed",
                "result": {
                    "version": document["version"],
                    "summary": summary,
                    "tags": tags,
                },
            },
            "$unset": {"active_slot": "", "lease_id": "", "lease_until": ""},
        },
    )
    if update_result.modified_count:
        try:
            await service.redis.set(
                cache_key(document["user_id"], document["content_hash"]),
                json.dumps({"summary": summary, "tags": tags}),
                ex=CACHE_SECONDS,
            )
        except RedisError:
            logger.exception("Could not cache completed result")
        await service.refresh_count_after_write(document["user_id"])
    return True


async def fail_stage(
    service: DocumentService, document: dict, job_filter: dict, stage: str
) -> None:
    update_result = await service.collection.update_one(
        job_filter,
        {
            "$set": {"status": "failed", f"stages.{stage}": "failed"},
            "$unset": {"active_slot": "", "lease_id": "", "lease_until": ""},
        },
    )
    if update_result.modified_count:
        await service.refresh_count_after_write(document["user_id"])


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    mongo = AsyncMongoClient(os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017"))
    redis = Redis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"))
    service = DocumentService(mongo, redis)
    try:
        while True:
            try:
                await service.ensure_indexes()
                if not await process_one(service):
                    await asyncio.sleep(1)
            except (PyMongoError, RedisError):
                logger.exception("Worker could not process a document")
                await asyncio.sleep(2)
    finally:
        await redis.aclose()
        await mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
