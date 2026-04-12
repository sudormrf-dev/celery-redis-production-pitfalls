"""Result backend patterns for Celery + Redis."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    """Celery task states."""

    PENDING = "PENDING"
    RECEIVED = "RECEIVED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REVOKED = "REVOKED"
    RETRY = "RETRY"

    def is_terminal(self) -> bool:
        return self in {TaskState.SUCCESS, TaskState.FAILURE, TaskState.REVOKED}

    def is_failure(self) -> bool:
        return self in {TaskState.FAILURE, TaskState.REVOKED}


class BackendType(str, Enum):
    """Supported result backend types."""

    REDIS = "redis"
    REDIS_SENTINEL = "redis-sentinel"
    REDIS_CLUSTER = "redis-cluster"
    CACHE = "cache"
    DATABASE = "db"
    DISABLED = "disabled"


@dataclass
class ResultBackendConfig:
    """Configuration for the result backend."""

    backend_type: BackendType = BackendType.REDIS
    url: str = "redis://localhost:6379/1"
    result_expires: int = 86400  # 1 day in seconds
    result_compression: str | None = None
    result_serializer: str = "json"
    redis_max_connections: int = 10
    redis_socket_timeout: float = 5.0
    redis_socket_connect_timeout: float = 2.0
    chord_unlock_max_retries: int = 60

    def to_celery_config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {
            "result_backend": self.url,
            "result_expires": self.result_expires,
            "result_serializer": self.result_serializer,
            "redis_max_connections": self.redis_max_connections,
            "redis_socket_timeout": self.redis_socket_timeout,
            "redis_socket_connect_timeout": self.redis_socket_connect_timeout,
            "chord_unlock_max_retries": self.chord_unlock_max_retries,
        }
        if self.result_compression:
            cfg["result_compression"] = self.result_compression
        return cfg


@dataclass
class TaskResult:
    """Stored result for a Celery task."""

    task_id: str
    state: TaskState = TaskState.PENDING
    result: Any = None
    error: str | None = None
    traceback: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    retries: int = 0
    runtime: float | None = None

    def mark_started(self) -> None:
        self.state = TaskState.STARTED
        self.updated_at = datetime.now(UTC)

    def mark_success(self, result: Any, runtime: float | None = None) -> None:
        self.state = TaskState.SUCCESS
        self.result = result
        self.runtime = runtime
        self.updated_at = datetime.now(UTC)

    def mark_failure(self, error: str, traceback: str | None = None) -> None:
        self.state = TaskState.FAILURE
        self.error = error
        self.traceback = traceback
        self.updated_at = datetime.now(UTC)

    def mark_retry(self, attempt: int) -> None:
        self.state = TaskState.RETRY
        self.retries = attempt
        self.updated_at = datetime.now(UTC)

    def is_ready(self) -> bool:
        return self.state.is_terminal()

    def age_seconds(self) -> float:
        return (datetime.now(UTC) - self.created_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "result": self.result,
            "error": self.error,
            "retries": self.retries,
            "runtime": self.runtime,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class ResultBackend:
    """In-memory result backend (mirrors Redis backend semantics)."""

    def __init__(self, config: ResultBackendConfig | None = None) -> None:
        self._config = config or ResultBackendConfig()
        self._results: dict[str, TaskResult] = {}

    def store(self, result: TaskResult) -> None:
        self._results[result.task_id] = result

    def get(self, task_id: str) -> TaskResult | None:
        return self._results.get(task_id)

    def forget(self, task_id: str) -> bool:
        if task_id in self._results:
            del self._results[task_id]
            return True
        return False

    def get_many(self, task_ids: list[str]) -> dict[str, TaskResult]:
        return {tid: self._results[tid] for tid in task_ids if tid in self._results}

    def pending_count(self) -> int:
        return sum(1 for r in self._results.values() if r.state == TaskState.PENDING)

    def failure_rate(self) -> float:
        if not self._results:
            return 0.0
        failures = sum(1 for r in self._results.values() if r.state.is_failure())
        return failures / len(self._results)

    def cleanup_expired(self, max_age_seconds: float | None = None) -> int:
        limit = (
            max_age_seconds
            if max_age_seconds is not None
            else float(self._config.result_expires)
        )
        expired = [tid for tid, r in self._results.items() if r.age_seconds() > limit]
        for tid in expired:
            del self._results[tid]
        return len(expired)

    def all_results(self) -> list[TaskResult]:
        return list(self._results.values())
