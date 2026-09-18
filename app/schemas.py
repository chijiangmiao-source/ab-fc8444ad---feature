from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class CreateSessionRequest(BaseModel):
    file_size: int = Field(gt=0, description="total file size in bytes")
    chunk_size: int = Field(gt=0, description="chunk size in bytes; every chunk except the last must be exactly this long")
    file_sha256: str = Field(description="SHA-256 of the whole file, hex encoded")
    expires_at: datetime = Field(description="ISO-8601 timestamp with timezone; new chunks are rejected afterwards")

    @field_validator("file_sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("file_sha256 must be exactly 64 hexadecimal characters")
        return value.lower()

    @field_validator("expires_at")
    @classmethod
    def _validate_expires_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("expires_at must include an explicit timezone")
        return value


class ReuseChunkRequest(BaseModel):
    sha256: str = Field(description="SHA-256 of the cached chunk content, lowercase hex")

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("sha256 must be exactly 64 lowercase hexadecimal characters")
        return value


class ReclaimRequest(BaseModel):
    max_blobs: int = Field(
        ge=1, le=1000, description="maximum number of pool candidates to examine in this cycle"
    )
