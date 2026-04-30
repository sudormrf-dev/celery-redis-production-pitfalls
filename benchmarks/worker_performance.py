"""Simulate worker throughput under different Celery configurations.

Benchmarks compare:
  - Concurrency levels : 1, 4, 16 workers
  - Result backend type: Redis vs disabled
  - Serializer         : json vs msgpack (simulated byte sizes)
  - Compression        : none vs gzip

All numbers are simulated from realistic latency models —
no real Redis or Celery process is required.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

from patterns.result_backend import BackendType, ResultBackendConfig
from patterns.worker_config import (
    ConcurrencyModel,
    WorkerConfig,
    WorkerPool,
    WorkerStats,
)


# ---------------------------------------------------------------------------
# Latency model constants (milliseconds)
# ---------------------------------------------------------------------------

_TASK_COMPUTE_MS: float = 8.0  # simulated CPU work per task
_REDIS_STORE_MS: float = 1.5  # result backend write latency
_BROKER_FETCH_MS: float = 2.0  # broker BLPOP per worker slot
_JSON_OVERHEAD_MS: float = 0.8  # JSON (de)serialization overhead
_MSGPACK_OVERHEAD_MS: float = 0.2  # msgpack overhead (4x faster)
_GZIP_OVERHEAD_MS: float = 1.2  # compression overhead


# ---------------------------------------------------------------------------
# Configuration descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BenchConfig:
    """One benchmark configuration to test."""

    label: str
    concurrency: int
    backend_type: BackendType
    serializer: str  # "json" | "msgpack"
    compression: str | None  # None | "gzip"
    model: ConcurrencyModel = ConcurrencyModel.PREFORK


# ---------------------------------------------------------------------------
# Throughput simulator
# ---------------------------------------------------------------------------


@dataclass
class ThroughputSimulator:
    """Estimate tasks/sec for a given BenchConfig over a simulated duration."""

    config: BenchConfig
    duration_seconds: float = 5.0
    _pool: WorkerPool = field(default_factory=WorkerPool, init=False)

    def _task_latency_ms(self) -> float:
        """Single-task wall-clock latency including all overheads."""
        latency = _TASK_COMPUTE_MS + _BROKER_FETCH_MS

        # Serializer overhead
        latency += (
            _MSGPACK_OVERHEAD_MS
            if self.config.serializer == "msgpack"
            else _JSON_OVERHEAD_MS
        )

        # Compression overhead
        if self.config.compression == "gzip":
            latency += _GZIP_OVERHEAD_MS

        # Result backend
        if self.config.backend_type == BackendType.REDIS:
            latency += _REDIS_STORE_MS
        # DISABLED backend → no write cost

        return latency

    def run(self) -> dict[str, Any]:
        """Simulate worker pool for `duration_seconds` and compute throughput."""
        # Register workers
        workers: list[WorkerStats] = []
        for i in range(self.config.concurrency):
            w = self._pool.register(f"worker-{i}")
            w.record_heartbeat()
            workers.append(w)

        task_latency_s = self._task_latency_ms() / 1_000.0
        # Each worker completes floor(duration / latency) tasks in parallel
        tasks_per_worker = math.floor(self.duration_seconds / task_latency_s)
        total_tasks = tasks_per_worker * self.config.concurrency

        # Record simulated stats for each worker
        for w in workers:
            for _ in range(tasks_per_worker):
                w.record_success(runtime_ms=self._task_latency_ms())
            w.record_heartbeat()

        throughput = total_tasks / self.duration_seconds
        return {
            "config": self.config.label,
            "concurrency": self.config.concurrency,
            "backend": self.config.backend_type.value,
            "serializer": self.config.serializer,
            "compression": self.config.compression or "none",
            "latency_ms": round(self._task_latency_ms(), 2),
            "tasks_simulated": total_tasks,
            "throughput_per_sec": round(throughput, 1),
            "success_rate": self._pool.overall_success_rate(),
        }


# ---------------------------------------------------------------------------
# Benchmark suite
# ---------------------------------------------------------------------------

BENCH_CONFIGS: list[BenchConfig] = [
    # --- Concurrency sweep (baseline: Redis backend, JSON, no compression) ---
    BenchConfig(
        label="1-worker  | redis | json   | no-gzip",
        concurrency=1,
        backend_type=BackendType.REDIS,
        serializer="json",
        compression=None,
    ),
    BenchConfig(
        label="4-workers | redis | json   | no-gzip",
        concurrency=4,
        backend_type=BackendType.REDIS,
        serializer="json",
        compression=None,
    ),
    BenchConfig(
        label="16-workers| redis | json   | no-gzip",
        concurrency=16,
        backend_type=BackendType.REDIS,
        serializer="json",
        compression=None,
    ),
    # --- Backend impact (4 workers) ---
    BenchConfig(
        label="4-workers | none  | json   | no-gzip",
        concurrency=4,
        backend_type=BackendType.DISABLED,
        serializer="json",
        compression=None,
    ),
    # --- Serializer impact (4 workers, Redis) ---
    BenchConfig(
        label="4-workers | redis | msgpack| no-gzip",
        concurrency=4,
        backend_type=BackendType.REDIS,
        serializer="msgpack",
        compression=None,
    ),
    # --- Compression impact (4 workers, Redis, JSON) ---
    BenchConfig(
        label="4-workers | redis | json   | gzip   ",
        concurrency=4,
        backend_type=BackendType.REDIS,
        serializer="json",
        compression="gzip",
    ),
    # --- Optimal config: 16 workers, disabled backend, msgpack ---
    BenchConfig(
        label="16-workers| none  | msgpack| no-gzip",
        concurrency=16,
        backend_type=BackendType.DISABLED,
        serializer="msgpack",
        compression=None,
    ),
]


def run_benchmarks(duration_seconds: float = 5.0) -> list[dict[str, Any]]:
    """Execute all benchmark configurations and return results."""
    results: list[dict[str, Any]] = []
    for cfg in BENCH_CONFIGS:
        sim = ThroughputSimulator(config=cfg, duration_seconds=duration_seconds)
        results.append(sim.run())
    return results


def print_table(results: list[dict[str, Any]]) -> None:
    """Print aligned benchmark results table."""
    col_labels = [
        "Configuration",
        "Workers",
        "Backend",
        "Serial.",
        "Compress",
        "Latency(ms)",
        "Tasks",
        "Tasks/sec",
    ]
    col_keys = [
        "config",
        "concurrency",
        "backend",
        "serializer",
        "compression",
        "latency_ms",
        "tasks_simulated",
        "throughput_per_sec",
    ]

    rows: list[list[str]] = []
    for r in results:
        rows.append([str(r[k]) for k in col_keys])

    col_widths = [
        max(len(label), max(len(row[i]) for row in rows)) + 2
        for i, label in enumerate(col_labels)
    ]

    separator = "  " + "-" * (sum(col_widths) + len(col_widths))
    header = "  " + "".join(
        label.ljust(col_widths[i]) for i, label in enumerate(col_labels)
    )

    print(separator)
    print(header)
    print(separator)
    for row in rows:
        print("  " + "".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row)))
    print(separator)


def print_insights(results: list[dict[str, Any]]) -> None:
    """Highlight key throughput observations."""
    print("\n  Key insights:")

    # Concurrency scaling
    r1 = next(
        r
        for r in results
        if r["concurrency"] == 1
        and r["serializer"] == "json"
        and r["backend"] == "redis"
    )
    r4 = next(
        r
        for r in results
        if r["concurrency"] == 4
        and r["serializer"] == "json"
        and r["backend"] == "redis"
        and r["compression"] == "none"
    )
    r16 = next(
        r
        for r in results
        if r["concurrency"] == 16
        and r["serializer"] == "json"
        and r["backend"] == "redis"
    )
    print(
        f"  • 4×  workers vs 1×  : {r4['throughput_per_sec'] / r1['throughput_per_sec']:.1f}x throughput"
    )
    print(
        f"  • 16× workers vs 1×  : {r16['throughput_per_sec'] / r1['throughput_per_sec']:.1f}x throughput"
    )

    # Backend impact
    r_disabled = next(
        r
        for r in results
        if r["concurrency"] == 4
        and r["backend"] == "disabled"
        and r["serializer"] == "json"
    )
    pct = (
        (r_disabled["throughput_per_sec"] - r4["throughput_per_sec"])
        / r4["throughput_per_sec"]
        * 100
    )
    print(f"  • Disabling result backend (+{pct:.0f}% throughput @ 4 workers)")

    # Serializer impact
    r_msgpack = next(
        r
        for r in results
        if r["concurrency"] == 4
        and r["serializer"] == "msgpack"
        and r["backend"] == "redis"
    )
    ser_pct = (
        (r_msgpack["throughput_per_sec"] - r4["throughput_per_sec"])
        / r4["throughput_per_sec"]
        * 100
    )
    print(
        f"  • msgpack vs json     : +{ser_pct:.0f}% throughput (lower (de)serialization cost)"
    )

    # Compression cost
    r_gzip = next(r for r in results if r["compression"] == "gzip")
    gz_pct = (
        (r4["throughput_per_sec"] - r_gzip["throughput_per_sec"])
        / r4["throughput_per_sec"]
        * 100
    )
    print(
        f"  • gzip compression    : -{gz_pct:.0f}% throughput (worthwhile only for large payloads)"
    )

    # Best config
    best = max(results, key=lambda r: r["throughput_per_sec"])
    print(
        f"  • Best config         : [{best['config'].strip()}] → {best['throughput_per_sec']} tasks/sec"
    )


def build_worker_config_from_bench(cfg: BenchConfig) -> WorkerConfig:
    """Build a WorkerConfig matching a BenchConfig for documentation purposes."""
    return WorkerConfig(
        concurrency=cfg.concurrency,
        model=cfg.model,
        prefetch_multiplier=4,
        max_tasks_per_child=1000,
        queues=["celery"],
    )


def build_backend_config_from_bench(cfg: BenchConfig) -> ResultBackendConfig:
    """Build a ResultBackendConfig matching a BenchConfig."""
    return ResultBackendConfig(
        backend_type=cfg.backend_type,
        result_expires=3600,
        result_serializer=cfg.serializer,
        result_compression=cfg.compression,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the benchmark suite and print results."""
    print("=" * 70)
    print("  Celery + Redis Worker Performance Benchmarks (simulated)")
    print("=" * 70)

    t0 = time.monotonic()
    results = run_benchmarks(duration_seconds=5.0)
    elapsed = time.monotonic() - t0

    print(f"\n  Simulation completed in {elapsed * 1000:.1f} ms\n")
    print_table(results)
    print_insights(results)

    print("\n  Celery config reference for optimal throughput:")
    best_cfg = max(
        BENCH_CONFIGS,
        key=lambda c: ThroughputSimulator(config=c, duration_seconds=5.0).run()[
            "throughput_per_sec"
        ],
    )
    wc = build_worker_config_from_bench(best_cfg)
    bc = build_backend_config_from_bench(best_cfg)
    print(f"    WorkerConfig  : concurrency={wc.concurrency}, model={wc.model.value}")
    print(
        f"    BackendConfig : type={bc.backend_type.value}, serializer={bc.result_serializer}"
    )

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
