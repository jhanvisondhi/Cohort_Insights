from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from pymongo.errors import ServerSelectionTimeoutError

from app.database import get_mongo_client
from app.main import app


def test_mongodb_health_reports_success() -> None:
    client = MagicMock()
    client.admin.command = AsyncMock(return_value={"ok": 1})
    app.dependency_overrides[get_mongo_client] = lambda: client
    try:
        with TestClient(app) as test_client:
            response = test_client.get("/health/mongodb")
    finally:
        app.dependency_overrides.pop(get_mongo_client, None)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    client.admin.command.assert_awaited_once_with("ping")


def test_mongodb_health_reports_unavailable() -> None:
    client = MagicMock()
    client.admin.command = AsyncMock(
        side_effect=ServerSelectionTimeoutError("connection failed")
    )
    app.dependency_overrides[get_mongo_client] = lambda: client
    try:
        with TestClient(app) as test_client:
            response = test_client.get("/health/mongodb")
    finally:
        app.dependency_overrides.pop(get_mongo_client, None)

    assert response.status_code == 503
    assert response.json() == {"detail": "MongoDB is unavailable."}


def test_lifespan_uses_configured_uri_and_closes_client(monkeypatch) -> None:
    monkeypatch.setenv("MONGODB_URI", "mongodb://example.test:27017")
    client = MagicMock()
    client.close = AsyncMock()
    with patch("app.database.AsyncMongoClient", return_value=client) as factory:
        with TestClient(app):
            assert app.state.mongo_client is client

    factory.assert_called_once_with(
        "mongodb://example.test:27017", serverSelectionTimeoutMS=2000
    )
    client.close.assert_awaited_once()
