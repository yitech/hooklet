from typing import Any, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class Msg(BaseModel):
    id: str = Field(alias="_id", default_factory=lambda: str(uuid4()))
    type: str
    data: Any
    error: Optional[str] = None


class Req(BaseModel):
    id: str = Field(alias="_id", default_factory=lambda: str(uuid4()))
    type: str
    params: Any
    error: Optional[str] = None


class Reply(BaseModel):
    id: str = Field(alias="_id", default_factory=lambda: str(uuid4()))
    type: str
    result: Any
    error: Optional[str] = None
    start_ms: int
    end_ms: int


class Job(BaseModel):
    id: str = Field(alias="_id", default_factory=lambda: str(uuid4()))
    type: str
    data: Any
    error: Optional[str] = None
    recv_ms: int
    start_ms: int
    end_ms: int
    status: Literal["pending", "new", "running", "finished", "failed", "cancelled"]
    retry_count: int
