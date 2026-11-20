import asyncio
import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient
from fastapi import HTTPException
from pymongo import AsyncMongoClient, MongoClient
from pymongo.errors import PyMongoError
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import RedisError

from app.main import app
from app.services.documents import DocumentService, count_key
from app.worker import process_one


@pytest.fixture
def environment(monkeypatch):
    mongo = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=1000)
    redis = Redis.from_url("redis://127.0.0.1:6379/0", socket_connect_timeout=1)
    try:
        mongo.admin.command("ping")
        redis.ping()
    except (PyMongoError, RedisError) as exc:
        mongo.close()
        redis.close()
        pytest.skip(f"MongoDB and Redis integration services are required: {exc}")

    suffix = uuid.uuid4().hex
    database = f"cohort_test_{suffix}"
    user = f"user-{suffix}"
    other = f"other-{suffix}"
    monkeypatch.setenv("MONGODB_URI", "mongodb://127.0.0.1:27017")
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    monkeypatch.setenv("MONGODB_DATABASE", database)
    monkeypatch.setenv("API_KEYS", json.dumps({user: "key-one", other: "key-two"}))
    with TestClient(app) as client:
        yield client, mongo, redis, database, user, other
    mongo.drop_database(database)
    for name in (user, other):
        digest = hashlib.sha256(name.encode()).hexdigest()
        for key in redis.scan_iter(match=f"cohort:*:{digest}*"):
            redis.delete(key)
    mongo.close()
    redis.close()


def submit(client, user, content, ref=None):
    payload = {"user_id": user, "title": "Test", "content": content}
    if ref is not None:
        payload["client_doc_ref"] = ref
    return client.post("/documents", headers={"X-API-Key": "key-one"}, json=payload)


async def run_worker(database, random_values):
    mongo = AsyncMongoClient("mongodb://127.0.0.1:27017")
    redis = AsyncRedis.from_url("redis://127.0.0.1:6379/0")
    try:
        service = DocumentService(mongo, redis)
        with patch("app.worker.sleep", new_callable=AsyncMock) as sleep:
            with patch("app.worker.random.random", side_effect=random_values):
                assert await process_one(service)
        return sleep.await_count
    finally:
        await redis.aclose()
        await mongo.close()


def test_submission_ownership_reference_limit_cache_and_index(environment):
    client, mongo, redis, database, user, other = environment
    first = submit(client, user, "Python coding practice", "ref-1")
    assert first.status_code == 201
    identifier = first.json()["document_id"]
    assert submit(client, user, "different content", "ref-1").status_code == 409
    assert submit(client, user, "second").status_code == 201
    assert submit(client, user, "third").status_code == 201
    assert submit(client, user, "fourth").status_code == 429
    assert int(redis.get(count_key(user))) == 3

    assert client.get(f"/documents/{identifier}").status_code == 401
    assert client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-two"}).status_code == 404
    assert client.get("/documents/by-ref/ref-1", headers={"X-API-Key": "key-two"}).status_code == 404
    found = client.get("/documents/by-ref/ref-1", headers={"X-API-Key": "key-one"})
    assert found.status_code == 200 and found.json()["document_id"] == identifier
    assert client.get(f"/users/{other}/documents", headers={"X-API-Key": "key-one"}).status_code == 404
    page = client.get(f"/users/{user}/documents?page=1&page_size=2", headers={"X-API-Key": "key-one"})
    assert page.status_code == 200 and len(page.json()["documents"]) == 2
    assert "IXSCAN" in json.dumps(
        mongo[database].documents.find(
            {"client_doc_ref": "ref-1", "user_id": user}
        ).hint("client_doc_ref_1").explain()
    )

    assert asyncio.run(run_worker(database, [0.9, 0.9])) == 2
    completed = client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-one"}).json()
    assert completed["status"] == "completed"
    assert completed["result"]["version"] == completed["version"] == 1
    assert completed["result"]["tags"]
    cached = submit(client, user, "Python coding practice")
    assert cached.status_code == 201
    assert cached.json()["status"] == "completed"
    assert cached.json()["result"]["summary"] == completed["result"]["summary"]
    assert int(redis.get(count_key(user))) == 2
    changed = client.patch(
        f"/documents/{identifier}",
        headers={"X-API-Key": "key-one"},
        json={"content": "Updated different content"},
    )
    assert changed.status_code == 200
    assert changed.json()["version"] == 2
    assert changed.json()["result"] is None
    assert int(redis.get(count_key(user))) == 3
    old_content = submit(client, user, "Python coding practice")
    assert old_content.json()["status"] == "completed"
    assert old_content.json()["result"]["summary"] == "Python coding practice"


def test_enrichment_failure_preserves_summary_and_retry_skips_processing(environment):
    client, _, _, database, user, _ = environment
    response = submit(client, user, "Summary for retry")
    identifier = response.json()["document_id"]
    assert asyncio.run(run_worker(database, [0.9, 0.0])) == 2
    failed = client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-one"}).json()
    assert failed["status"] == "failed"
    assert failed["stages"] == {"processing": "completed", "enriching": "failed"}
    assert failed["result"]["summary"] == "Summary for retry"
    assert failed["result"]["tags"] is None
    retried = client.post(f"/documents/{identifier}/retry", headers={"X-API-Key": "key-one"})
    assert retried.status_code == 200
    assert asyncio.run(run_worker(database, [0.9])) == 1
    completed = client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-one"}).json()
    assert completed["status"] == "completed"
    assert completed["result"]["summary"] == failed["result"]["summary"]


def test_update_racing_old_worker_never_publishes_old_result(environment):
    client, _, _, database, user, _ = environment
    response = submit(client, user, "Old content")
    identifier = response.json()["document_id"]

    async def race():
        mongo = AsyncMongoClient("mongodb://127.0.0.1:27017")
        redis = AsyncRedis.from_url("redis://127.0.0.1:6379/0")
        service = DocumentService(mongo, redis)
        started = asyncio.Event()
        proceed = asyncio.Event()

        async def pause(_seconds):
            started.set()
            await proceed.wait()

        try:
            with patch("app.worker.sleep", side_effect=pause):
                with patch("app.worker.random.random", return_value=0.9):
                    task = asyncio.create_task(process_one(service))
                    await asyncio.wait_for(started.wait(), timeout=5)
                    updated = await service.update(user, identifier, "New content")
                    assert updated["version"] == 2 and updated["result"] is None
                    proceed.set()
                    assert await task
            current = await service.get(user, identifier)
            assert current["status"] == "queued" and current["result"] is None
        finally:
            await redis.aclose()
            await mongo.close()

    asyncio.run(race())
    asyncio.run(run_worker(database, [0.9, 0.9]))
    current = client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-one"}).json()
    assert current["version"] == current["result"]["version"] == 2
    assert current["result"]["summary"] == "New content"


def test_concurrent_submissions_do_not_exceed_three_active(environment):
    client, _, _, _, user, _ = environment
    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(lambda n: submit(client, user, f"content {n}"), range(6)))
    assert sorted(response.status_code for response in responses) == [201, 201, 201, 429, 429, 429]


def test_worker_recovers_an_expired_processing_lease(environment):
    client, mongo, _, database, user, _ = environment
    response = submit(client, user, "Recover a paused job")
    identifier = response.json()["document_id"]
    mongo[database].documents.update_one(
        {"_id": ObjectId(identifier)},
        {"$set": {
            "status": "processing",
            "stages.processing": "running",
            "lease_id": "expired",
            "lease_until": datetime.now(timezone.utc) - timedelta(seconds=1),
        }},
    )
    asyncio.run(run_worker(database, [0.9, 0.9]))
    current = client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-one"}).json()
    assert current["status"] == "completed"
    assert current["result"]["summary"] == "Recover a paused job"


def test_processing_failure_and_retry(environment):
    client, _, _, database, user, _ = environment
    identifier = submit(client, user, "Try processing again").json()["document_id"]
    asyncio.run(run_worker(database, [0.0]))
    failed = client.get(f"/documents/{identifier}", headers={"X-API-Key": "key-one"}).json()
    assert failed["stages"] == {"processing": "failed", "enriching": "pending"}
    assert failed["result"] is None
    assert client.post(
        f"/documents/{identifier}/retry", headers={"X-API-Key": "key-one"}
    ).status_code == 200
    asyncio.run(run_worker(database, [0.9, 0.9]))
    assert client.get(
        f"/documents/{identifier}", headers={"X-API-Key": "key-one"}
    ).json()["status"] == "completed"


def test_concurrent_updates_from_same_version_conflict(environment):
    client, _, _, _, user, _ = environment
    identifier = submit(client, user, "Original content").json()["document_id"]

    async def race():
        mongo = AsyncMongoClient("mongodb://127.0.0.1:27017")
        redis = AsyncRedis.from_url("redis://127.0.0.1:6379/0")
        service = DocumentService(mongo, redis)
        original = service.collection
        both_read = asyncio.Event()

        class PausedCollection:
            reads = 0

            def __getattr__(self, name):
                return getattr(original, name)

            async def find_one(self, *args, **kwargs):
                result = await original.find_one(*args, **kwargs)
                self.reads += 1
                if self.reads <= 2:
                    if self.reads == 2:
                        both_read.set()
                    await asyncio.wait_for(both_read.wait(), timeout=5)
                return result

        service.collection = PausedCollection()
        try:
            outcomes = await asyncio.gather(
                service.update(user, identifier, "First update"),
                service.update(user, identifier, "Second update"),
                return_exceptions=True,
            )
            failures = [outcome for outcome in outcomes if isinstance(outcome, HTTPException)]
            successes = [outcome for outcome in outcomes if isinstance(outcome, dict)]
            assert len(successes) == 1 and successes[0]["version"] == 2
            assert len(failures) == 1 and failures[0].status_code == 409
        finally:
            await redis.aclose()
            await mongo.close()

    asyncio.run(race())
