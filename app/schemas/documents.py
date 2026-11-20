from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DocumentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: Annotated[str, Field(min_length=1, max_length=128)]
    title: Annotated[str, Field(min_length=1, max_length=200)]
    content: Annotated[str, Field(min_length=1, max_length=100_000)]
    client_doc_ref: Annotated[str, Field(min_length=1, max_length=256)] | None = None

    @field_validator("user_id", "title", "content", "client_doc_ref")
    @classmethod
    def reject_blank_values(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("must not be blank")
        return value


class DocumentSubmission(BaseModel):
    document_id: str
    status: Literal["queued", "completed"]
    result: dict[str, object] | None = None


class DocumentUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: Annotated[str, Field(min_length=1, max_length=100_000)]

    @field_validator("content")
    @classmethod
    def reject_blank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value
