"""Task configuration patterns for production Celery."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class AckPolicy(str, Enum):
    """When to acknowledge a task message."""

    ACK_LATE = "ack_late"
    ACK_EARLY = "ack_early"
    REJECT_ON_WORKER_LOST = "reject_on_worker_lost"


class BackoffStrategy(str, Enum):
    """Retry backoff strategies."""

    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    JITTER = "jitter"


class QueuePriority(str, Enum):
    """Logical queue priority tiers."""

    CRITICAL = "critical"
    HIGH = "high"
    DEFAULT = "default"
    LOW = "low"
    BULK = "bulk"

    def routing_key(self) -> str:
        return f"tasks.{self.value}"


@dataclass
class RetryConfig:
    """Retry configuration for a Celery task."""

    max_retries: int = 3
    default_retry_delay: float = 60.0
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay: float = 2.0
    max_delay: float = 3600.0
    jitter_range: float = 30.0

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate retry delay for given attempt (1-indexed)."""
        if attempt < 1:
            attempt = 1
        if self.strategy == BackoffStrategy.FIXED:
            delay = self.default_retry_delay
        elif self.strategy == BackoffStrategy.LINEAR:
            delay = self.base_delay * attempt
        elif self.strategy == BackoffStrategy.JITTER:
            import random

            delay = self.default_retry_delay + random.uniform(0, self.jitter_range)
        else:
            # EXPONENTIAL
            delay = self.base_delay**attempt
        return min(delay, self.max_delay)

    def should_retry(self, attempt: int) -> bool:
        return attempt <= self.max_retries


@dataclass
class TaskConfig:
    """Production-ready Celery task configuration."""

    name: str
    queue: QueuePriority = QueuePriority.DEFAULT
    ack_policy: AckPolicy = AckPolicy.ACK_LATE
    retry: RetryConfig = field(default_factory=RetryConfig)
    time_limit: int = 300
    soft_time_limit: int = 270
    serializer: str = "json"
    compression: str | None = None
    rate_limit: str | None = None
    ignore_result: bool = False
    expires: int | None = None
    priority: int = 5
    _extra: dict[str, Any] = field(default_factory=dict)

    def to_task_kwargs(self) -> dict[str, Any]:
        """Return kwargs suitable for @app.task(...)."""
        kwargs: dict[str, Any] = {
            "name": self.name,
            "queue": self.queue.value,
            "acks_late": self.ack_policy == AckPolicy.ACK_LATE,
            "reject_on_worker_lost": self.ack_policy == AckPolicy.REJECT_ON_WORKER_LOST,
            "max_retries": self.retry.max_retries,
            "default_retry_delay": self.retry.default_retry_delay,
            "time_limit": self.time_limit,
            "soft_time_limit": self.soft_time_limit,
            "serializer": self.serializer,
            "ignore_result": self.ignore_result,
            "priority": self.priority,
        }
        if self.compression:
            kwargs["compression"] = self.compression
        if self.rate_limit:
            kwargs["rate_limit"] = self.rate_limit
        if self.expires:
            kwargs["expires"] = self.expires
        kwargs.update(self._extra)
        return kwargs

    def with_extra(self, key: str, value: Any) -> TaskConfig:
        self._extra[key] = value
        return self


class TaskConfigBuilder:
    """Fluent builder for TaskConfig."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._queue = QueuePriority.DEFAULT
        self._ack = AckPolicy.ACK_LATE
        self._retry = RetryConfig()
        self._time_limit = 300
        self._soft_time_limit = 270
        self._rate_limit: str | None = None
        self._ignore_result = False

    def queue(self, q: QueuePriority) -> TaskConfigBuilder:
        self._queue = q
        return self

    def ack_policy(self, policy: AckPolicy) -> TaskConfigBuilder:
        self._ack = policy
        return self

    def retries(
        self, max_retries: int, strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    ) -> TaskConfigBuilder:
        self._retry = RetryConfig(max_retries=max_retries, strategy=strategy)
        return self

    def time_limits(self, hard: int, soft: int | None = None) -> TaskConfigBuilder:
        self._time_limit = hard
        self._soft_time_limit = soft if soft is not None else max(1, hard - 30)
        return self

    def rate_limit(self, limit: str) -> TaskConfigBuilder:
        self._rate_limit = limit
        return self

    def ignore_result(self, ignore: bool = True) -> TaskConfigBuilder:
        self._ignore_result = ignore
        return self

    def build(self) -> TaskConfig:
        return TaskConfig(
            name=self._name,
            queue=self._queue,
            ack_policy=self._ack,
            retry=self._retry,
            time_limit=self._time_limit,
            soft_time_limit=self._soft_time_limit,
            rate_limit=self._rate_limit,
            ignore_result=self._ignore_result,
        )
