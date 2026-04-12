"""Tests for connection_pool.py."""

from __future__ import annotations

import time

from patterns.connection_pool import (
    CircuitBreaker,
    CircuitState,
    ConnectionPoolConfig,
    PoolStats,
    RedisConnectionPool,
)


class TestConnectionPoolConfig:
    def test_default_host(self):
        cfg = ConnectionPoolConfig()
        assert cfg.host == "localhost"

    def test_to_url_no_password(self):
        cfg = ConnectionPoolConfig(host="redis", port=6379, db=0)
        url = cfg.to_url()
        assert url == "redis://redis:6379/0"

    def test_to_url_with_password(self):
        cfg = ConnectionPoolConfig(password="secret")
        url = cfg.to_url()
        assert ":secret@" in url

    def test_ssl_uses_rediss(self):
        cfg = ConnectionPoolConfig(ssl=True)
        assert cfg.to_url().startswith("rediss://")

    def test_to_redis_kwargs(self):
        cfg = ConnectionPoolConfig()
        kwargs = cfg.to_redis_kwargs()
        assert "max_connections" in kwargs
        assert kwargs["socket_keepalive"] is True


class TestPoolStats:
    def test_utilization_zero_when_empty(self):
        s = PoolStats()
        assert s.utilization == 0.0

    def test_utilization_calculated(self):
        s = PoolStats(total_connections=10, in_use_connections=5)
        assert s.utilization == 0.5

    def test_not_saturated_below_threshold(self):
        s = PoolStats(total_connections=10, in_use_connections=5)
        assert s.is_saturated() is False

    def test_saturated_at_threshold(self):
        s = PoolStats(total_connections=10, in_use_connections=9)
        assert s.is_saturated() is True


class TestRedisConnectionPool:
    def test_acquire_success(self):
        pool = RedisConnectionPool(ConnectionPoolConfig(max_connections=5))
        assert pool.acquire() is True

    def test_acquire_exhausted(self):
        pool = RedisConnectionPool(ConnectionPoolConfig(max_connections=2))
        pool.acquire()
        pool.acquire()
        assert pool.acquire() is False

    def test_release_decrements(self):
        pool = RedisConnectionPool()
        pool.acquire()
        pool.release()
        assert pool.stats().in_use_connections == 0

    def test_peak_tracked(self):
        pool = RedisConnectionPool()
        pool.acquire()
        pool.acquire()
        pool.release()
        assert pool.stats().peak_connections == 2

    def test_commands_processed(self):
        pool = RedisConnectionPool()
        pool.acquire()
        pool.acquire()
        assert pool.stats().commands_processed == 2

    def test_healthy_initially(self):
        pool = RedisConnectionPool()
        assert pool.is_healthy() is True

    def test_set_unhealthy(self):
        pool = RedisConnectionPool()
        pool.set_healthy(False)
        assert pool.is_healthy() is False

    def test_record_error(self):
        pool = RedisConnectionPool()
        for _ in range(10):
            pool.record_error()
        assert pool.is_healthy() is False

    def test_config_accessible(self):
        cfg = ConnectionPoolConfig(host="myredis")
        pool = RedisConnectionPool(cfg)
        assert pool.config.host == "myredis"


class TestCircuitBreaker:
    def test_initial_state_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED

    def test_allow_request_when_closed(self):
        cb = CircuitBreaker()
        assert cb.allow_request() is True

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_open_blocks_requests(self):
        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure()
        assert cb.allow_request() is False

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)
        assert cb.state == CircuitState.HALF_OPEN

    def test_success_closes_from_half_open(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.state  # trigger half-open
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_failure_in_half_open_reopens(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)
        _ = cb.state  # trigger half-open
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_reset(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count() == 0

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(failure_threshold=5)
        cb.record_failure()
        cb.record_success()
        assert cb.failure_count() == 0
