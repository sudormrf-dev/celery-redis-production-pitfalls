"""Tests for result_backend.py."""

from __future__ import annotations

import time

from patterns.result_backend import (
    BackendType,
    ResultBackend,
    ResultBackendConfig,
    TaskResult,
    TaskState,
)


class TestTaskState:
    def test_success_is_terminal(self):
        assert TaskState.SUCCESS.is_terminal() is True

    def test_failure_is_terminal(self):
        assert TaskState.FAILURE.is_terminal() is True

    def test_pending_not_terminal(self):
        assert TaskState.PENDING.is_terminal() is False

    def test_started_not_terminal(self):
        assert TaskState.STARTED.is_terminal() is False

    def test_revoked_is_failure(self):
        assert TaskState.REVOKED.is_failure() is True

    def test_success_not_failure(self):
        assert TaskState.SUCCESS.is_failure() is False


class TestResultBackendConfig:
    def test_default_url(self):
        cfg = ResultBackendConfig()
        assert "redis" in cfg.url

    def test_to_celery_config_has_key(self):
        cfg = ResultBackendConfig()
        d = cfg.to_celery_config()
        assert "result_backend" in d
        assert d["result_expires"] == 86400

    def test_compression_included(self):
        cfg = ResultBackendConfig(result_compression="gzip")
        d = cfg.to_celery_config()
        assert d["result_compression"] == "gzip"

    def test_disabled_backend_type(self):
        cfg = ResultBackendConfig(backend_type=BackendType.DISABLED)
        assert cfg.backend_type == BackendType.DISABLED


class TestTaskResult:
    def test_initial_state_pending(self):
        r = TaskResult(task_id="abc")
        assert r.state == TaskState.PENDING

    def test_mark_started(self):
        r = TaskResult(task_id="abc")
        r.mark_started()
        assert r.state == TaskState.STARTED

    def test_mark_success(self):
        r = TaskResult(task_id="abc")
        r.mark_success({"output": 42}, runtime=0.5)
        assert r.state == TaskState.SUCCESS
        assert r.result == {"output": 42}
        assert r.runtime == 0.5

    def test_mark_failure(self):
        r = TaskResult(task_id="abc")
        r.mark_failure("ValueError: bad input", "traceback here")
        assert r.state == TaskState.FAILURE
        assert r.error == "ValueError: bad input"
        assert r.traceback == "traceback here"

    def test_mark_retry(self):
        r = TaskResult(task_id="abc")
        r.mark_retry(2)
        assert r.state == TaskState.RETRY
        assert r.retries == 2

    def test_is_ready_true_after_success(self):
        r = TaskResult(task_id="abc")
        r.mark_success(None)
        assert r.is_ready() is True

    def test_is_ready_false_pending(self):
        r = TaskResult(task_id="abc")
        assert r.is_ready() is False

    def test_age_seconds_positive(self):
        r = TaskResult(task_id="abc")
        time.sleep(0.01)
        assert r.age_seconds() > 0

    def test_to_dict(self):
        r = TaskResult(task_id="abc")
        d = r.to_dict()
        assert d["task_id"] == "abc"
        assert "state" in d
        assert "created_at" in d


class TestResultBackend:
    def test_store_and_get(self):
        b = ResultBackend()
        r = TaskResult(task_id="x1")
        b.store(r)
        assert b.get("x1") is r

    def test_get_missing_returns_none(self):
        b = ResultBackend()
        assert b.get("missing") is None

    def test_forget(self):
        b = ResultBackend()
        b.store(TaskResult(task_id="del"))
        assert b.forget("del") is True
        assert b.get("del") is None

    def test_forget_missing_returns_false(self):
        b = ResultBackend()
        assert b.forget("no") is False

    def test_get_many(self):
        b = ResultBackend()
        b.store(TaskResult(task_id="a"))
        b.store(TaskResult(task_id="b"))
        results = b.get_many(["a", "b", "c"])
        assert len(results) == 2

    def test_pending_count(self):
        b = ResultBackend()
        b.store(TaskResult(task_id="p1"))
        b.store(TaskResult(task_id="p2"))
        r3 = TaskResult(task_id="p3")
        r3.mark_success(None)
        b.store(r3)
        assert b.pending_count() == 2

    def test_failure_rate(self):
        b = ResultBackend()
        r1 = TaskResult(task_id="f1")
        r1.mark_failure("err")
        r2 = TaskResult(task_id="s1")
        r2.mark_success(None)
        b.store(r1)
        b.store(r2)
        assert b.failure_rate() == 0.5

    def test_empty_failure_rate(self):
        assert ResultBackend().failure_rate() == 0.0

    def test_cleanup_expired(self):
        b = ResultBackend(ResultBackendConfig(result_expires=86400))
        r = TaskResult(task_id="old")
        b.store(r)
        removed = b.cleanup_expired(max_age_seconds=0.0)
        assert removed == 1

    def test_all_results(self):
        b = ResultBackend()
        b.store(TaskResult(task_id="x"))
        b.store(TaskResult(task_id="y"))
        assert len(b.all_results()) == 2
