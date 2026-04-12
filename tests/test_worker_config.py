"""Tests for worker_config.py."""

from __future__ import annotations

import time

from patterns.worker_config import (
    ConcurrencyModel,
    WorkerConfig,
    WorkerMonitor,
    WorkerPool,
    WorkerStats,
)


class TestConcurrencyModel:
    def test_prefork_not_async(self):
        assert ConcurrencyModel.PREFORK.is_async() is False

    def test_gevent_is_async(self):
        assert ConcurrencyModel.GEVENT.is_async() is True

    def test_eventlet_is_async(self):
        assert ConcurrencyModel.EVENTLET.is_async() is True


class TestWorkerConfig:
    def test_default_concurrency(self):
        cfg = WorkerConfig()
        assert cfg.concurrency == 4

    def test_to_celery_args_includes_concurrency(self):
        cfg = WorkerConfig(concurrency=8)
        args = cfg.to_celery_worker_args()
        assert "--concurrency" in args
        idx = args.index("--concurrency")
        assert args[idx + 1] == "8"

    def test_to_celery_args_includes_queues(self):
        cfg = WorkerConfig(queues=["high", "default"])
        args = cfg.to_celery_worker_args()
        assert "--queues" in args
        idx = args.index("--queues")
        assert "high" in args[idx + 1]

    def test_without_flags(self):
        cfg = WorkerConfig(without_heartbeat=True, without_gossip=True)
        args = cfg.to_celery_worker_args()
        assert "--without-heartbeat" in args
        assert "--without-gossip" in args

    def test_optimal_prefetch_async(self):
        cfg = WorkerConfig(model=ConcurrencyModel.GEVENT, prefetch_multiplier=4)
        assert cfg.optimal_prefetch() == 1

    def test_optimal_prefetch_prefork(self):
        cfg = WorkerConfig(
            concurrency=4, prefetch_multiplier=4, model=ConcurrencyModel.PREFORK
        )
        assert cfg.optimal_prefetch() == 16

    def test_max_memory_included(self):
        cfg = WorkerConfig(max_memory_per_child=100_000)
        args = cfg.to_celery_worker_args()
        assert "--max-memory-per-child" in args


class TestWorkerStats:
    def test_initial_totals_zero(self):
        s = WorkerStats(worker_id="w1")
        assert s.total_tasks == 0

    def test_record_success(self):
        s = WorkerStats(worker_id="w1")
        s.record_success(100.0)
        assert s.tasks_succeeded == 1
        assert s.total_runtime_ms == 100.0

    def test_record_failure(self):
        s = WorkerStats(worker_id="w1")
        s.record_failure()
        assert s.tasks_failed == 1

    def test_success_rate_all_success(self):
        s = WorkerStats(worker_id="w1")
        s.record_success(50.0)
        s.record_success(60.0)
        assert s.success_rate() == 1.0

    def test_success_rate_mixed(self):
        s = WorkerStats(worker_id="w1")
        s.record_success(50.0)
        s.record_failure()
        assert s.success_rate() == 0.5

    def test_no_tasks_success_rate_1(self):
        s = WorkerStats(worker_id="w1")
        assert s.success_rate() == 1.0

    def test_mean_runtime(self):
        s = WorkerStats(worker_id="w1")
        s.record_success(100.0)
        s.record_success(200.0)
        assert s.mean_runtime_ms() == 150.0

    def test_mean_runtime_no_tasks(self):
        s = WorkerStats(worker_id="w1")
        assert s.mean_runtime_ms() == 0.0

    def test_uptime_seconds_positive(self):
        s = WorkerStats(worker_id="w1")
        time.sleep(0.01)
        assert s.uptime_seconds() > 0

    def test_not_alive_without_heartbeat(self):
        s = WorkerStats(worker_id="w1")
        assert s.is_alive() is False

    def test_alive_after_heartbeat(self):
        s = WorkerStats(worker_id="w1")
        s.record_heartbeat()
        assert s.is_alive(timeout=60.0) is True

    def test_not_alive_after_timeout(self):
        s = WorkerStats(worker_id="w1")
        s.record_heartbeat()
        assert s.is_alive(timeout=0.0) is False


class TestWorkerPool:
    def test_register_returns_stats(self):
        pool = WorkerPool()
        stats = pool.register("w1")
        assert isinstance(stats, WorkerStats)

    def test_get_registered(self):
        pool = WorkerPool()
        pool.register("w1")
        assert pool.get("w1") is not None

    def test_get_missing_none(self):
        pool = WorkerPool()
        assert pool.get("missing") is None

    def test_unregister(self):
        pool = WorkerPool()
        pool.register("w1")
        assert pool.unregister("w1") is True
        assert pool.get("w1") is None

    def test_unregister_missing(self):
        pool = WorkerPool()
        assert pool.unregister("no") is False

    def test_total_tasks(self):
        pool = WorkerPool()
        w = pool.register("w1")
        w.record_success(10.0)
        w.record_success(20.0)
        assert pool.total_tasks_processed() == 2

    def test_overall_success_rate(self):
        pool = WorkerPool()
        w = pool.register("w1")
        w.record_success(10.0)
        w.record_failure()
        assert pool.overall_success_rate() == 0.5

    def test_worker_count(self):
        pool = WorkerPool()
        pool.register("w1")
        pool.register("w2")
        assert pool.worker_count() == 2


class TestWorkerMonitor:
    def test_check_health_no_workers(self):
        pool = WorkerPool()
        monitor = WorkerMonitor(pool)
        issues = monitor.check_health(min_alive=1)
        assert len(issues) > 0

    def test_check_health_ok(self):
        pool = WorkerPool()
        w = pool.register("w1")
        w.record_heartbeat()
        w.record_success(10.0)
        monitor = WorkerMonitor(pool)
        issues = monitor.check_health(min_alive=1)
        assert len(issues) == 0

    def test_summary_keys(self):
        pool = WorkerPool()
        monitor = WorkerMonitor(pool)
        s = monitor.summary()
        assert "worker_count" in s
        assert "success_rate" in s
