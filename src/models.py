from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    topic: str = Field(..., min_length=1, description="Nama topik, tidak boleh kosong")
    event_id: str = Field(..., min_length=1, description="Pengidentifikasi unik event (UUID atau nanoid)")
    timestamp: datetime = Field(..., description="Stempel waktu kejadian dalam format ISO8601")
    source: str = Field(..., min_length=1, description="Sumber event")
    payload: dict[str, Any] | None = Field(default=None, description="Objek payload bersarang (opsional)")

    @field_validator("topic", "event_id", "source", mode="before")
    @classmethod
    def must_not_be_blank(cls, v: Any) -> Any:
        if isinstance(v, str) and not v.strip():
            raise ValueError("Field tidak boleh kosong atau hanya berisi spasi")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "topic": "user-signup",
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "timestamp": "2026-04-20T10:00:00Z",
                "source": "auth-service",
                "payload": {"user_id": 42, "plan": "free"},
            }
        }
    }


class PublishResponse(BaseModel):
    accepted: int
    rejected: int


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime_seconds: float
