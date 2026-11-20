from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError

from app.database import get_redis_client
from app.main import app


def test_redis_health_reports_success() -> None:
    client = MagicMock()
    client.ping = AsyncMock(return_value=True)
    app.dependency_overrides[get_redis_client] = lambda: client
    try:
        with TestClient(app) as test_client:
            response = test_client.get("/health/redis")
    finally:
        app.dependency_overrides.pop(get_redis_client, None)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    client.ping.assert_awaited_once()


def test_redis_health_reports_unavailable() -> None:
    client = MagicMock()
    client.ping = AsyncMock(side_effect=ConnectionError("connection failed"))
    app.dependency_overrides[get_redis_client] = lambda: client
    try:
        with TestClient(app) as test_client:
            response = test_client.get("/health/redis")
    finally:
        app.dependency_overrides.pop(get_redis_client, None)

    assert response.status_code == 503
    assert response.json() == {"detail": "Redis is unavailable."}


def test_lifespan_uses_configured_redis_url_and_closes_client(monkeypatch) -> None:
    monkeypatch.setenv("REDIS_URL", "redis://example.test:6379/0")
    client = MagicMock()
    client.aclose = AsyncMock()
    with patch("app.database.Redis.from_url", return_value=client) as factory:
        with TestClient(app):
            assert app.state.redis_client is client

    factory.assert_called_once_with(
        "redis://example.test:6379/0", socket_connect_timeout=2, socket_timeout=2
    )
    client.aclose.assert_awaited_once()
