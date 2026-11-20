import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Annotated, Any

from bson import ObjectId
from fastapi import Depends, HTTPException
from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.errors import DuplicateKeyError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.database import get_mongo_client, get_redis_client
from app.schemas.documents import DocumentCreate, DocumentSubmission

logger = logging.getLogger(__name__)
ACTIVE = ("queued", "processing", "enriching")
CACHE_SECONDS = 24 * 60 * 60
MAX_ACTIVE_DOCUMENTS = 3


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def cache_key(user_id: str, digest: str) -> str:
    user_hash = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
    return f"cohort:cache:{user_hash}:{digest}"


def count_key(user_id: str) -> str:
    user_hash = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
    return f"cohort:active:{user_hash}"


def document_id(value: str) -> ObjectId:
    if not ObjectId.is_valid(value):
        raise HTTPException(status_code=404, detail="Document not found.")
    return ObjectId(value)


def public_document(doc: dict[str, Any]) -> dict[str, Any]:
    result = doc.get("result")
    if result is not None and result["version"] != doc["version"]:
        result = None
    return {
        "document_id": str(doc["_id"]),
        "user_id": doc["user_id"],
        "title": doc["title"],
        "content": doc["content"],
        "client_doc_ref": doc.get("client_doc_ref"),
        "status": doc["status"],
        "version": doc["version"],
        "content_hash": doc["content_hash"],
        "result": result,
        "stages": doc["stages"],
        "created_at": doc["created_at"],
    }


class DocumentService:
    def __init__(self, mongo: AsyncMongoClient, redis: Redis) -> None:
        database_name = os.environ.get("MONGODB_DATABASE", "cohort_insights")
        database = mongo[database_name]
        self.collection = database["documents"]
        self.redis = redis

    async def ensure_indexes(self) -> None:
        await self.collection.create_index(
            [("client_doc_ref", ASCENDING)],
            unique=True,
            partialFilterExpression={"client_doc_ref": {"$type": "string"}},
        )
        await self.collection.create_index(
            [("user_id", ASCENDING), ("active_slot", ASCENDING)],
            unique=True,
            partialFilterExpression={"active_slot": {"$exists": True}},
        )
        await self.collection.create_index(
            [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)]
        )
        await self.collection.create_index(
            [("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING)]
        )
        await self.collection.create_index(
            [("user_id", ASCENDING), ("content_hash", ASCENDING)]
        )
        await self.collection.create_index([("status", ASCENDING), ("lease_until", ASCENDING)])

    async def refresh_count(self, user_id: str) -> None:
        async with self.redis.lock(
            f"{count_key(user_id)}:lock", timeout=10, blocking_timeout=2
        ):
            count = await self.collection.count_documents(
                {"user_id": user_id, "status": {"$in": list(ACTIVE)}}
            )
            await self.redis.set(count_key(user_id), count, ex=CACHE_SECONDS)

    async def refresh_count_after_write(self, user_id: str) -> None:
        try:
            await self.refresh_count(user_id)
        except RedisError:
            logger.exception("Could not refresh Redis active count")

    async def cached_result(self, user_id: str, digest: str) -> dict[str, Any] | None:
        cached = await self.redis.get(cache_key(user_id, digest))
        if cached is None:
            return None
        try:
            result = json.loads(cached)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RedisError("Invalid cached result") from exc
        if (
            not isinstance(result, dict)
            or not isinstance(result.get("summary"), str)
            or not isinstance(result.get("tags"), list)
            or not all(isinstance(tag, str) for tag in result["tags"])
        ):
            raise RedisError("Invalid cached result")
        return result

    async def submit(self, document: DocumentCreate) -> DocumentSubmission:
        await self.ensure_indexes()
        await self.refresh_count(document.user_id)
        digest = content_hash(document.content)
        cached = await self.cached_result(document.user_id, digest)
        if cached is None:
            previous = await self.collection.find_one(
                {
                    "user_id": document.user_id,
                    "content_hash": digest,
                    "status": "completed",
                }
            )
            if previous is not None and previous["result"]["version"] == previous["version"]:
                cached = {
                    "summary": previous["result"]["summary"],
                    "tags": previous["result"]["tags"],
                }
                await self.redis.set(
                    cache_key(document.user_id, digest),
                    json.dumps(cached),
                    ex=CACHE_SECONDS,
                )
        has_cached_result = cached is not None
        stage_status = "completed" if has_cached_result else "pending"
        record: dict[str, Any] = {
            "user_id": document.user_id,
            "title": document.title,
            "content": document.content,
            "content_hash": digest,
            "version": 1,
            "created_at": datetime.now(timezone.utc),
            "status": "completed" if has_cached_result else "queued",
            "result": {"version": 1, **cached} if cached is not None else None,
            "stages": {
                "processing": stage_status,
                "enriching": stage_status,
            },
        }
        if document.client_doc_ref is not None:
            record["client_doc_ref"] = document.client_doc_ref

        slots = [None] if has_cached_result else range(MAX_ACTIVE_DOCUMENTS)
        for slot in slots:
            if slot is not None:
                record["active_slot"] = slot
            try:
                inserted = await self.collection.insert_one(record)
                await self.refresh_count_after_write(document.user_id)
                return DocumentSubmission(
                    document_id=str(inserted.inserted_id),
                    status=record["status"],
                    result=record["result"],
                )
            except DuplicateKeyError:
                if document.client_doc_ref is not None and await self.collection.find_one(
                    {"client_doc_ref": document.client_doc_ref}, {"_id": 1},
                    hint="client_doc_ref_1",
                ):
                    raise HTTPException(status_code=409, detail="External reference already exists.")
                if has_cached_result:
                    raise
        raise HTTPException(status_code=429, detail="Maximum active documents reached.")

    async def get(self, user_id: str, identifier: str) -> dict[str, Any]:
        doc = await self.collection.find_one(
            {"_id": document_id(identifier), "user_id": user_id}
        )
        if doc is None:
            raise HTTPException(status_code=404, detail="Document not found.")
        return public_document(doc)

    async def by_ref(self, user_id: str, ref: str) -> dict[str, Any]:
        await self.ensure_indexes()
        doc = await self.collection.find_one(
            {"client_doc_ref": ref, "user_id": user_id}, hint="client_doc_ref_1"
        )
        if doc is None:
            raise HTTPException(status_code=404, detail="Document not found.")
        return public_document(doc)

    async def list_documents(
        self, user_id: str, page: int, page_size: int, status: str | None
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"user_id": user_id}
        if status:
            query["status"] = status
        cursor = (
            self.collection.find(query)
            .sort([("created_at", DESCENDING), ("_id", DESCENDING)])
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        return [public_document(doc) async for doc in cursor]

    async def update(self, user_id: str, identifier: str, content: str) -> dict[str, Any]:
        await self.ensure_indexes()
        await self.refresh_count(user_id)
        doc = await self.collection.find_one({"_id": document_id(identifier), "user_id": user_id})
        if doc is None:
            raise HTTPException(status_code=404, detail="Document not found.")
        version = doc["version"] + 1
        fields = {
            "content": content,
            "content_hash": content_hash(content),
            "version": version,
            "status": "queued",
            "result": None,
            "stages": {"processing": "pending", "enriching": "pending"},
        }
        slots = list(range(MAX_ACTIVE_DOCUMENTS))
        if "active_slot" in doc:
            slots.remove(doc["active_slot"])
            slots.insert(0, doc["active_slot"])
        for slot in slots:
            try:
                updated = await self.collection.update_one(
                    {"_id": doc["_id"], "user_id": user_id, "version": doc["version"]},
                    {
                        "$set": {**fields, "active_slot": slot},
                        "$unset": {"lease_id": "", "lease_until": ""},
                    },
                )
                if updated.modified_count == 0:
                    raise HTTPException(status_code=409, detail="Document changed; retry update.")
                await self.refresh_count_after_write(user_id)
                return await self.get(user_id, identifier)
            except DuplicateKeyError:
                continue
        raise HTTPException(status_code=429, detail="Maximum active documents reached.")

    async def retry(self, user_id: str, identifier: str) -> dict[str, Any]:
        await self.ensure_indexes()
        await self.refresh_count(user_id)
        doc = await self.collection.find_one({"_id": document_id(identifier), "user_id": user_id})
        if doc is None:
            raise HTTPException(status_code=404, detail="Document not found.")
        if doc["status"] != "failed":
            raise HTTPException(status_code=409, detail="Only failed documents can be retried.")
        failed_stage = (
            "enriching" if doc["stages"]["enriching"] == "failed" else "processing"
        )
        for slot in range(MAX_ACTIVE_DOCUMENTS):
            try:
                updated = await self.collection.update_one(
                    {"_id": doc["_id"], "user_id": user_id, "version": doc["version"], "status": "failed"},
                    {
                        "$set": {
                            "status": "queued" if failed_stage == "processing" else "enriching",
                            f"stages.{failed_stage}": "pending",
                            "active_slot": slot,
                        },
                        "$unset": {"lease_id": "", "lease_until": ""},
                    },
                )
                if updated.modified_count == 0:
                    raise HTTPException(status_code=409, detail="Document changed; retry.")
                await self.refresh_count_after_write(user_id)
                return await self.get(user_id, identifier)
            except DuplicateKeyError:
                continue
        raise HTTPException(status_code=429, detail="Maximum active documents reached.")


def get_document_service(
    mongo: Annotated[AsyncMongoClient, Depends(get_mongo_client)],
    redis: Annotated[Redis, Depends(get_redis_client)],
) -> DocumentService:
    return DocumentService(mongo, redis)
