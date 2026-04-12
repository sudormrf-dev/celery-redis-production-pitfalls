"""Worker configuration and monitoring patterns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class ConcurrencyModel(str, Enum):
    """Worker concurrency models."""

    PREFORK = "prefork"
    EVENTLET = "eventlet"
    GEVENT = "gevent"
    SOLO = "solo"
    THREADS = "threads"

    def is_async(self) -> bool:
        return self in {ConcurrencyModel.EVENTLET, ConcurrencyModel.GEVENT}


@dataclass
class WorkerConfig:
    """Production worker configuration."""

    concurrency: int = 4
    model: ConcurrencyModel = ConcurrencyModel.PREFORK
    prefetch_multiplier: int = 4
    max_tasks_per_child: int = 1000
    max_memory_per_child: int = 200_000  # kB
    queues: list[str] = field(default_factory=lambda: ["celery"])
    hostname: str = "worker@%h"
    loglevel: str = "INFO"
    without_heartbeat: bool = False
    without_gossip: bool = False
    without_mingle: bool = False
    heartbeat_interval: float = 2.0
    pool_timeout: float = 4.0

    def to_celery_worker_args(self) -> list[str]:
        args = [
            "--concurrency",
            str(self.concurrency),
            "--pool",
            self.model.value,
            "--prefetch-multiplier",
            str(self.prefetch_multiplier),
            "--max-tasks-per-child",
            str(self.max_tasks_per_child),
            "--queues",
            ",".join(self.queues),
            "--hostname",
            self.hostname,
            "--loglevel",
            self.loglevel,
        ]
        if self.max_memory_per_child:
            args += ["--max-memory-per-child", str(self.max_memory_per_child)]
        if self.without_heartbeat:
            args.append("--without-heartbeat")
        if self.without_gossip:
            args.append("--without-gossip")
        if self.without_mingle:
            args.append("--without-mingle")
        return args

    def optimal_prefetch(self) -> int:
        """Recommended prefetch for this concurrency model."""
        if self.model.is_async():
            return 1
        return self.prefetch_multiplier * self.concurrency


@dataclass
class WorkerStats:
    """Runtime statistics for a worker."""

    worker_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    tasks_retried: int = 0
    tasks_rejected: int = 0
    total_runtime_ms: float = 0.0
    heartbeat_at: datetime | None = None

    def record_success(self, runtime_ms: float) -> None:
        self.tasks_succeeded += 1
        self.total_runtime_ms += runtime_ms

    def record_failure(self) -> None:
        self.tasks_failed += 1

    def record_retry(self) -> None:
        self.tasks_retried += 1

    def record_heartbeat(self) -> None:
        self.heartbeat_at = datetime.now(UTC)

    @property
    def total_tasks(self) -> int:
        return self.tasks_succeeded + self.tasks_failed

    def success_rate(self) -> float:
        if self.total_tasks == 0:
            return 1.0
        return self.tasks_succeeded / self.total_tasks

    def mean_runtime_ms(self) -> float:
        if self.tasks_succeeded == 0:
            return 0.0
        return self.total_runtime_ms / self.tasks_succeeded

    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()

    def is_alive(self, timeout: float = 30.0) -> bool:
        if self.heartbeat_at is None:
            return False
        return (datetime.now(UTC) - self.heartbeat_at).total_seconds() < timeout


class WorkerPool:
    """Registry of worker instances."""

    def __init__(self) -> None:
        self._workers: dict[str, WorkerStats] = {}

    def register(self, worker_id: str) -> WorkerStats:
        stats = WorkerStats(worker_id=worker_id)
        self._workers[worker_id] = stats
        return stats

    def get(self, worker_id: str) -> WorkerStats | None:
        return self._workers.get(worker_id)

    def unregister(self, worker_id: str) -> bool:
        if worker_id in self._workers:
            del self._workers[worker_id]
            return True
        return False

    def alive_workers(self, timeout: float = 30.0) -> list[WorkerStats]:
        return [w for w in self._workers.values() if w.is_alive(timeout)]

    def total_tasks_processed(self) -> int:
        return sum(w.total_tasks for w in self._workers.values())

    def overall_success_rate(self) -> float:
        total = sum(w.total_tasks for w in self._workers.values())
        if total == 0:
            return 1.0
        succeeded = sum(w.tasks_succeeded for w in self._workers.values())
        return succeeded / total

    def worker_count(self) -> int:
        return len(self._workers)


class WorkerMonitor:
    """Monitor for worker health and performance."""

    def __init__(self, pool: WorkerPool) -> None:
        self._pool = pool
        self._alerts: list[dict[str, Any]] = []

    def check_health(
        self,
        min_alive: int = 1,
        min_success_rate: float = 0.95,
    ) -> list[str]:
        issues: list[str] = []
        alive = len(self._pool.alive_workers())
        if alive < min_alive:
            issues.append(f"Only {alive} workers alive (min={min_alive})")
        rate = self._pool.overall_success_rate()
        if rate < min_success_rate:
            issues.append(
                f"Success rate {rate:.1%} below threshold {min_success_rate:.1%}"
            )
        return issues

    def summary(self) -> dict[str, Any]:
        return {
            "worker_count": self._pool.worker_count(),
            "alive_count": len(self._pool.alive_workers()),
            "total_tasks": self._pool.total_tasks_processed(),
            "success_rate": self._pool.overall_success_rate(),
        }
