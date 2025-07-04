from typing import Any, Literal, TypedDict


class Msg(TypedDict):
    _id: str
    type: str
    data: Any
    error: str | None


class Req(TypedDict):
    _id: str
    type: str
    params: Any
    error: str | None


class Reply(TypedDict):
    _id: str
    type: str
    result: Any
    error: str | None
    start_ms: int
    end_ms: int


class Job(TypedDict):
    _id: str
    type: str
    data: Any
    error: str | None
    recv_ms: int
    start_ms: int
    end_ms: int
    status: Literal["new", "running", "finished", "failed", "cancelled"]
    retry_count: int
