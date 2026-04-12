"""Tests for task_config.py."""

from __future__ import annotations

from patterns.task_config import (
    AckPolicy,
    BackoffStrategy,
    QueuePriority,
    RetryConfig,
    TaskConfig,
    TaskConfigBuilder,
)


class TestQueuePriority:
    def test_routing_key_critical(self):
        assert QueuePriority.CRITICAL.routing_key() == "tasks.critical"

    def test_routing_key_default(self):
        assert QueuePriority.DEFAULT.routing_key() == "tasks.default"

    def test_all_have_routing_keys(self):
        for q in QueuePriority:
            assert q.routing_key().startswith("tasks.")


class TestRetryConfig:
    def test_default_max_retries(self):
        r = RetryConfig()
        assert r.max_retries == 3

    def test_should_retry_within_limit(self):
        r = RetryConfig(max_retries=3)
        assert r.should_retry(1) is True
        assert r.should_retry(3) is True
        assert r.should_retry(4) is False

    def test_fixed_delay(self):
        r = RetryConfig(strategy=BackoffStrategy.FIXED, default_retry_delay=60.0)
        assert r.delay_for_attempt(1) == 60.0
        assert r.delay_for_attempt(3) == 60.0

    def test_exponential_delay_grows(self):
        r = RetryConfig(strategy=BackoffStrategy.EXPONENTIAL, base_delay=2.0)
        d1 = r.delay_for_attempt(1)
        d2 = r.delay_for_attempt(2)
        assert d2 > d1

    def test_linear_delay(self):
        r = RetryConfig(strategy=BackoffStrategy.LINEAR, base_delay=10.0)
        assert r.delay_for_attempt(2) == 20.0
        assert r.delay_for_attempt(3) == 30.0

    def test_max_delay_capped(self):
        r = RetryConfig(
            strategy=BackoffStrategy.EXPONENTIAL, base_delay=100.0, max_delay=500.0
        )
        assert r.delay_for_attempt(10) <= 500.0

    def test_jitter_delay_positive(self):
        r = RetryConfig(
            strategy=BackoffStrategy.JITTER, default_retry_delay=60.0, jitter_range=10.0
        )
        assert r.delay_for_attempt(1) >= 60.0

    def test_attempt_zero_clamped(self):
        r = RetryConfig(strategy=BackoffStrategy.LINEAR, base_delay=5.0)
        assert r.delay_for_attempt(0) > 0


class TestTaskConfig:
    def test_default_name(self):
        c = TaskConfig(name="my_task")
        assert c.name == "my_task"

    def test_to_task_kwargs_has_name(self):
        c = TaskConfig(name="test")
        kwargs = c.to_task_kwargs()
        assert kwargs["name"] == "test"

    def test_ack_late_true(self):
        c = TaskConfig(name="t", ack_policy=AckPolicy.ACK_LATE)
        assert c.to_task_kwargs()["acks_late"] is True

    def test_ack_early_false(self):
        c = TaskConfig(name="t", ack_policy=AckPolicy.ACK_EARLY)
        assert c.to_task_kwargs()["acks_late"] is False

    def test_rate_limit_included(self):
        c = TaskConfig(name="t", rate_limit="10/s")
        assert c.to_task_kwargs()["rate_limit"] == "10/s"

    def test_compression_not_included_when_none(self):
        c = TaskConfig(name="t")
        assert "compression" not in c.to_task_kwargs()

    def test_with_extra(self):
        c = TaskConfig(name="t")
        c.with_extra("track_started", True)
        assert c.to_task_kwargs()["track_started"] is True

    def test_queue_value_in_kwargs(self):
        c = TaskConfig(name="t", queue=QueuePriority.HIGH)
        assert c.to_task_kwargs()["queue"] == "high"


class TestTaskConfigBuilder:
    def test_build_returns_task_config(self):
        cfg = TaskConfigBuilder("worker.send_email").build()
        assert cfg.name == "worker.send_email"

    def test_queue_set(self):
        cfg = TaskConfigBuilder("t").queue(QueuePriority.CRITICAL).build()
        assert cfg.queue == QueuePriority.CRITICAL

    def test_retries_set(self):
        cfg = TaskConfigBuilder("t").retries(5).build()
        assert cfg.retry.max_retries == 5

    def test_time_limits(self):
        cfg = TaskConfigBuilder("t").time_limits(600).build()
        assert cfg.time_limit == 600
        assert cfg.soft_time_limit < 600

    def test_time_limits_explicit_soft(self):
        cfg = TaskConfigBuilder("t").time_limits(300, 250).build()
        assert cfg.soft_time_limit == 250

    def test_rate_limit(self):
        cfg = TaskConfigBuilder("t").rate_limit("100/m").build()
        assert cfg.rate_limit == "100/m"

    def test_ignore_result(self):
        cfg = TaskConfigBuilder("t").ignore_result().build()
        assert cfg.ignore_result is True

    def test_chaining_returns_self(self):
        b = TaskConfigBuilder("t")
        assert b.queue(QueuePriority.LOW) is b

    def test_retries_strategy(self):
        cfg = TaskConfigBuilder("t").retries(3, BackoffStrategy.FIXED).build()
        assert cfg.retry.strategy == BackoffStrategy.FIXED
