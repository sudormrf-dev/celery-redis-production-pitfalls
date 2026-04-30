"""Demonstrate a production-grade Celery task pipeline.

Shows correct configuration versus five classic anti-patterns:
  1. Missing retry         — task crash → permanent loss
  2. No timeout            — task hangs forever
  3. Missing rate limiting — Redis saturation under burst load
  4. Ignoring task state   — results silently lost
  5. No result expiry      — unbounded memory growth
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from patterns.result_backend import (
    BackendType,
    ResultBackend,
    ResultBackendConfig,
    TaskResult,
    TaskState,
)
from patterns.task_config import (
    BackoffStrategy,
    QueuePriority,
    RetryConfig,
    TaskConfig,
    TaskConfigBuilder,
)

# ---------------------------------------------------------------------------
# Simulated task executor (no real Celery required)
# ---------------------------------------------------------------------------


@dataclass
class SimulatedTask:
    """Represents a task executing under a given TaskConfig."""

    config: TaskConfig
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    _attempt: int = field(default=0, init=False)

    def run(self, *, should_fail: bool = False, hang: bool = False) -> TaskResult:
        """Execute the task with simulated failure modes."""
        result = TaskResult(task_id=self.task_id)
        result.mark_started()

        if hang and self.config.time_limit == 0:
            # Anti-pattern 2: no timeout → task hangs indefinitely (simulated)
            result.mark_failure("Task timed out (no limit set — hung forever)")
            return result

        if hang:
            # Correct: soft_time_limit raises SoftTimeLimitExceeded → retry or fail fast
            result.mark_failure(
                f"SoftTimeLimitExceeded after {self.config.soft_time_limit}s — retrying"
            )
            return result

        if should_fail:
            self._attempt += 1
            if self.config.retry.should_retry(self._attempt):
                delay = self.config.retry.delay_for_attempt(self._attempt)
                result.mark_retry(self._attempt)
                result.error = (
                    f"Attempt {self._attempt} failed — retrying in {delay:.1f}s "
                    f"(strategy={self.config.retry.strategy.value})"
                )
            else:
                result.mark_failure(
                    f"Exhausted {self.config.retry.max_retries} retries"
                )
            return result

        result.mark_success({"status": "ok", "task": self.config.name}, runtime=0.42)
        return result


# ---------------------------------------------------------------------------
# Anti-pattern 1: Missing retry
# ---------------------------------------------------------------------------


def demo_missing_retry() -> None:
    """Anti-pattern: task crashes with no retry → permanent data loss."""
    print("\n--- Anti-pattern 1: Missing retry ---")
    bad_config = TaskConfig(
        name="process_payment",
        retry=RetryConfig(max_retries=0),  # no retries at all
    )
    task = SimulatedTask(config=bad_config)
    result = task.run(should_fail=True)
    print(f"  [BAD]  state={result.state.value}  error={result.error}")

    print("--- Fix: Exponential backoff retry ---")
    good_config = (
        TaskConfigBuilder("process_payment")
        .retries(3, strategy=BackoffStrategy.EXPONENTIAL)
        .queue(QueuePriority.CRITICAL)
        .build()
    )
    task2 = SimulatedTask(config=good_config)
    result2 = task2.run(should_fail=True)
    print(f"  [GOOD] state={result2.state.value}  error={result2.error}")


# ---------------------------------------------------------------------------
# Anti-pattern 2: No timeout
# ---------------------------------------------------------------------------


def demo_no_timeout() -> None:
    """Anti-pattern: task hangs forever, blocking a worker slot."""
    print("\n--- Anti-pattern 2: No timeout ---")
    bad_config = TaskConfig(
        name="fetch_external_api",
        time_limit=0,  # no hard limit
        soft_time_limit=0,  # no soft limit
    )
    task = SimulatedTask(config=bad_config)
    result = task.run(hang=True)
    print(f"  [BAD]  state={result.state.value}  error={result.error}")

    print("--- Fix: hard + soft time limits ---")
    good_config = (
        TaskConfigBuilder("fetch_external_api")
        .time_limits(hard=30, soft=25)
        .retries(2)
        .build()
    )
    task2 = SimulatedTask(config=good_config)
    result2 = task2.run(hang=True)
    print(f"  [GOOD] state={result2.state.value}  error={result2.error}")
    print(
        f"         hard_limit={good_config.time_limit}s  soft_limit={good_config.soft_time_limit}s"
    )


# ---------------------------------------------------------------------------
# Anti-pattern 3: Missing rate limiting
# ---------------------------------------------------------------------------


def demo_missing_rate_limit() -> None:
    """Anti-pattern: burst of tasks saturates Redis connection pool."""
    print("\n--- Anti-pattern 3: Missing rate limiting ---")
    bad_config = TaskConfig(
        name="send_notification",
        rate_limit=None,  # unlimited throughput → Redis saturation
    )
    burst = 500
    print(f"  [BAD]  Dispatching {burst} tasks with no rate limit → Redis overwhelmed")
    print(f"         rate_limit={bad_config.rate_limit!r}  (None = unlimited)")

    print("--- Fix: explicit rate limit ---")
    good_config = (
        TaskConfigBuilder("send_notification")
        .rate_limit("100/m")  # max 100 tasks per minute
        .retries(2)
        .build()
    )
    print(
        f"  [GOOD] rate_limit={good_config.rate_limit!r}  — Redis protected from burst"
    )


# ---------------------------------------------------------------------------
# Anti-pattern 4: Ignoring task state (fire and forget)
# ---------------------------------------------------------------------------


def demo_ignoring_task_state(backend: ResultBackend) -> None:
    """Anti-pattern: dispatch task, never check result → silent failures."""
    print("\n--- Anti-pattern 4: Ignoring task state ---")
    TaskConfig(
        name="resize_image",
        ignore_result=True,  # result never stored → failures invisible
    )
    task_id = str(uuid.uuid4())
    bad_result = TaskResult(task_id=task_id)
    bad_result.mark_failure("S3 upload failed: connection reset")
    # With ignore_result=True the backend is never consulted — failure is silent.
    print("  [BAD]  ignore_result=True → failure swallowed silently")
    print(f"         actual state would be: {bad_result.state.value}")

    print("--- Fix: track state in result backend ---")
    good_config = TaskConfigBuilder("resize_image").retries(3).build()
    task = SimulatedTask(config=good_config)
    result = task.run(should_fail=True)
    backend.store(result)
    stored = backend.get(result.task_id)
    assert stored is not None
    print(
        f"  [GOOD] state={stored.state.value}  retries={stored.retries}  tracked in backend"
    )


# ---------------------------------------------------------------------------
# Anti-pattern 5: No result expiry → memory leak
# ---------------------------------------------------------------------------


def demo_no_result_expiry(backend: ResultBackend) -> None:
    """Anti-pattern: results accumulate in Redis until OOM."""
    print("\n--- Anti-pattern 5: No result expiry ---")
    bad_backend_cfg = ResultBackendConfig(
        backend_type=BackendType.REDIS,
        result_expires=0,  # 0 = never expire → unbounded growth
    )
    print(
        f"  [BAD]  result_expires={bad_backend_cfg.result_expires}  (never expires → memory leak)"
    )

    print("--- Fix: bounded TTL + periodic cleanup ---")
    good_backend_cfg = ResultBackendConfig(
        backend_type=BackendType.REDIS,
        result_expires=3600,  # 1 hour TTL
    )
    print(f"  [GOOD] result_expires={good_backend_cfg.result_expires}s  (1 hour TTL)")

    # Simulate cleanup: insert 10 old results with age > TTL, clean up
    old_backend = ResultBackend(config=good_backend_cfg)
    for _ in range(10):
        r = TaskResult(task_id=str(uuid.uuid4()))
        r.mark_success("done")
        old_backend.store(r)
    # Force-expire all (max_age=0 → everything is old)
    removed = old_backend.cleanup_expired(max_age_seconds=0)
    print(f"         Cleaned up {removed} expired results — Redis memory reclaimed")


# ---------------------------------------------------------------------------
# Dead letter queue simulation
# ---------------------------------------------------------------------------


@dataclass
class DeadLetterQueue:
    """Collects tasks that exhausted all retries."""

    _items: list[dict[str, Any]] = field(default_factory=list)

    def push(self, task_id: str, task_name: str, error: str) -> None:
        self._items.append({"task_id": task_id, "task_name": task_name, "error": error})

    def drain(self) -> list[dict[str, Any]]:
        items, self._items = self._items, []
        return items

    def size(self) -> int:
        return len(self._items)


def demo_dead_letter_queue(backend: ResultBackend) -> None:
    """Demonstrate routing exhausted tasks to a dead-letter queue."""
    print("\n--- Dead Letter Queue: handling exhausted retries ---")
    dlq: DeadLetterQueue = DeadLetterQueue()

    config = (
        TaskConfigBuilder("import_csv")
        .retries(2, strategy=BackoffStrategy.EXPONENTIAL)
        .queue(QueuePriority.LOW)
        .build()
    )

    # Simulate exhausting all retries
    task = SimulatedTask(config=config)
    for attempt in range(1, config.retry.max_retries + 2):  # +1 to exceed max
        result = task.run(should_fail=True)
        backend.store(result)
        if result.state == TaskState.FAILURE:
            dlq.push(result.task_id, config.name, result.error or "unknown")
            print(f"  Attempt {attempt}: {result.state.value} — routed to DLQ")
            break
        print(f"  Attempt {attempt}: {result.state.value}  ({result.error})")

    dead = dlq.drain()
    print(
        f"  DLQ contains {len(dead)} item(s): {dead[0]['task_name']} → {dead[0]['error']}"
    )


# ---------------------------------------------------------------------------
# Monitoring summary
# ---------------------------------------------------------------------------


def demo_monitoring(backend: ResultBackend) -> None:
    """Show how to surface pipeline health metrics."""
    print("\n--- Monitoring: pipeline health ---")
    total = len(backend.all_results())
    failure_rate = backend.failure_rate()
    pending = backend.pending_count()
    successes = sum(1 for r in backend.all_results() if r.state == TaskState.SUCCESS)
    print(f"  total results  : {total}")
    print(f"  successes      : {successes}")
    print(f"  failure rate   : {failure_rate:.1%}")
    print(f"  pending        : {pending}")
    if failure_rate > 0.2:
        print("  [ALERT] failure rate exceeds 20% — check DLQ and worker logs")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run all anti-pattern demonstrations."""
    print("=" * 60)
    print("  Celery + Redis Production Pitfalls — Task Pipeline Demo")
    print("=" * 60)

    backend = ResultBackend(
        config=ResultBackendConfig(
            backend_type=BackendType.REDIS,
            result_expires=86400,
            result_serializer="json",
        )
    )

    demo_missing_retry()
    demo_no_timeout()
    demo_missing_rate_limit()
    demo_ignoring_task_state(backend)
    demo_no_result_expiry(backend)
    demo_dead_letter_queue(backend)
    demo_monitoring(backend)

    print("\n" + "=" * 60)
    print("  All 5 anti-patterns demonstrated and corrected.")
    print("=" * 60)


if __name__ == "__main__":
    main()
