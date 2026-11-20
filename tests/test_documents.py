import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError

from app.auth import current_user
from app.main import app
from app.schemas.documents import DocumentCreate, DocumentSubmission
from app.services.documents import get_document_service


@pytest.fixture
def payload() -> dict[str, str]:
    return {
        "user_id": "user-123",
        "title": "Learning Python",
        "content": "Python can be used to build APIs.",
    }


@pytest.fixture(autouse=True)
def authenticate():
    app.dependency_overrides[current_user] = lambda: "user-123"
    yield
    app.dependency_overrides.pop(current_user, None)


def test_submission_reports_unavailable_without_storage(payload: dict[str, str]) -> None:
    class UnavailableService:
        async def submit(self, document: DocumentCreate) -> DocumentSubmission:
            raise ConnectionError("connection failed")

    app.dependency_overrides[get_document_service] = lambda: UnavailableService()
    try:
        with TestClient(app) as client:
            response = client.post("/documents", json=payload)
    finally:
        app.dependency_overrides.pop(get_document_service, None)

    assert response.status_code == 503
    assert response.json() == {"detail": "Redis is unavailable."}


@pytest.mark.parametrize("field", ["user_id", "title", "content"])
def test_submission_rejects_missing_required_field(
    payload: dict[str, str], field: str
) -> None:
    del payload[field]
    with TestClient(app) as client:
        response = client.post("/documents", json=payload)

    assert response.status_code == 422


@pytest.mark.parametrize("field", ["user_id", "title", "content", "client_doc_ref"])
def test_submission_rejects_blank_values(payload: dict[str, str], field: str) -> None:
    payload[field] = "   "
    with TestClient(app) as client:
        response = client.post("/documents", json=payload)

    assert response.status_code == 422


def test_submission_rejects_unknown_fields(payload: dict[str, str]) -> None:
    payload["unexpected"] = "value"
    with TestClient(app) as client:
        response = client.post("/documents", json=payload)

    assert response.status_code == 422


def test_submission_passes_valid_data_to_service(payload: dict[str, str]) -> None:
    class FakeDocumentService:
        received: DocumentCreate | None = None

        async def submit(self, document: DocumentCreate) -> DocumentSubmission:
            self.received = document
            return DocumentSubmission(document_id="test-id", status="queued")

    service = FakeDocumentService()
    payload["client_doc_ref"] = "partner-123"
    payload["content"] = "  Keep the original content.  "
    app.dependency_overrides[get_document_service] = lambda: service
    try:
        with TestClient(app) as client:
            response = client.post("/documents", json=payload)
    finally:
        app.dependency_overrides.pop(get_document_service, None)

    assert response.status_code == 201
    assert response.json() == {"document_id": "test-id", "status": "queued"}
    assert service.received is not None
    assert service.received.model_dump() == payload
