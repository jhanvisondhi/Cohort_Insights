from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.auth import current_user
from app.schemas.documents import DocumentCreate, DocumentSubmission, DocumentUpdate
from app.services.documents import DocumentService, get_document_service

router = APIRouter(tags=["Documents"])
AuthenticatedUser = Annotated[str, Depends(current_user)]
DocumentServiceDependency = Annotated[DocumentService, Depends(get_document_service)]


@router.post(
    "/documents",
    response_model=DocumentSubmission,
    response_model_exclude_none=True,
    status_code=status.HTTP_201_CREATED,
)
async def submit_document(
    document: DocumentCreate, user: AuthenticatedUser, service: DocumentServiceDependency
) -> DocumentSubmission:
    if document.user_id != user:
        raise HTTPException(status_code=404, detail="User not found.")
    return await service.submit(document)


@router.get("/documents/by-ref/{client_doc_ref}")
async def get_by_ref(
    client_doc_ref: str, user: AuthenticatedUser, service: DocumentServiceDependency
) -> dict:
    return await service.by_ref(user, client_doc_ref)


@router.get("/documents/{document_id}")
async def get_document(
    document_id: str, user: AuthenticatedUser, service: DocumentServiceDependency
) -> dict:
    return await service.get(user, document_id)


@router.patch("/documents/{document_id}")
async def update_document(
    document_id: str,
    document: DocumentUpdate,
    user: AuthenticatedUser,
    service: DocumentServiceDependency,
) -> dict:
    return await service.update(user, document_id, document.content)


@router.post("/documents/{document_id}/retry")
async def retry_document(
    document_id: str, user: AuthenticatedUser, service: DocumentServiceDependency
) -> dict:
    return await service.retry(user, document_id)


@router.get("/users/{user_id}/documents")
async def list_documents(
    user_id: str,
    user: AuthenticatedUser,
    service: DocumentServiceDependency,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status_filter: Annotated[
        Literal["queued", "processing", "enriching", "completed", "failed"] | None,
        Query(alias="status"),
    ] = None,
) -> dict:
    if user_id != user:
        raise HTTPException(status_code=404, detail="User not found.")
    return {
        "page": page,
        "page_size": page_size,
        "documents": await service.list_documents(user, page, page_size, status_filter),
    }
