"""Redis connection pool and circuit breaker patterns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class ConnectionPoolConfig:
    """Redis connection pool configuration."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None
    max_connections: int = 50
    min_idle_connections: int = 5
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 2.0
    socket_keepalive: bool = True
    retry_on_timeout: bool = True
    health_check_interval: int = 30
    decode_responses: bool = True
    ssl: bool = False

    def to_url(self) -> str:
        scheme = "rediss" if self.ssl else "redis"
        auth = f":{self.password}@" if self.password else ""
        return f"{scheme}://{auth}{self.host}:{self.port}/{self.db}"

    def to_redis_kwargs(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "password": self.password,
            "max_connections": self.max_connections,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "socket_keepalive": self.socket_keepalive,
            "retry_on_timeout": self.retry_on_timeout,
            "health_check_interval": self.health_check_interval,
            "decode_responses": self.decode_responses,
            "ssl": self.ssl,
        }


@dataclass
class PoolStats:
    """Snapshot of connection pool statistics."""

    total_connections: int = 0
    idle_connections: int = 0
    in_use_connections: int = 0
    peak_connections: int = 0
    connection_errors: int = 0
    commands_processed: int = 0

    @property
    def utilization(self) -> float:
        if self.total_connections == 0:
            return 0.0
        return self.in_use_connections / self.total_connections

    def is_saturated(self, threshold: float = 0.9) -> bool:
        return self.utilization >= threshold


class RedisConnectionPool:
    """Simulated Redis connection pool with health tracking."""

    def __init__(self, config: ConnectionPoolConfig | None = None) -> None:
        self._config = config or ConnectionPoolConfig()
        self._stats = PoolStats()
        self._healthy = True

    @property
    def config(self) -> ConnectionPoolConfig:
        return self._config

    def acquire(self) -> bool:
        """Acquire a connection. Returns False if pool exhausted."""
        if self._stats.in_use_connections >= self._config.max_connections:
            return False
        self._stats.in_use_connections += 1
        self._stats.total_connections = max(
            self._stats.total_connections, self._stats.in_use_connections
        )
        self._stats.peak_connections = max(
            self._stats.peak_connections, self._stats.in_use_connections
        )
        self._stats.commands_processed += 1
        return True

    def release(self) -> None:
        """Release a connection back to pool."""
        if self._stats.in_use_connections > 0:
            self._stats.in_use_connections -= 1
            self._stats.idle_connections = max(
                0,
                self._config.min_idle_connections - self._stats.in_use_connections,
            )

    def record_error(self) -> None:
        self._stats.connection_errors += 1

    def stats(self) -> PoolStats:
        return PoolStats(
            total_connections=self._stats.total_connections,
            idle_connections=self._stats.idle_connections,
            in_use_connections=self._stats.in_use_connections,
            peak_connections=self._stats.peak_connections,
            connection_errors=self._stats.connection_errors,
            commands_processed=self._stats.commands_processed,
        )

    def is_healthy(self) -> bool:
        return self._healthy and self._stats.connection_errors < 10

    def set_healthy(self, healthy: bool) -> None:
        self._healthy = healthy


@dataclass
class CircuitBreaker:
    """Circuit breaker for Redis operations."""

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 3
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: datetime | None = field(default=None, init=False)
    _half_open_calls: int = field(default=0, init=False)

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN and self._last_failure_time is not None:
            elapsed = (datetime.now(UTC) - self._last_failure_time).total_seconds()
            if elapsed >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    def allow_request(self) -> bool:
        s = self.state
        if s == CircuitState.CLOSED:
            return True
        if s == CircuitState.OPEN:
            return False
        # HALF_OPEN
        return self._half_open_calls < self.half_open_max_calls

    def record_success(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
        self._failure_count = 0

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = datetime.now(UTC)
        if (
            self._state == CircuitState.HALF_OPEN
            or self._failure_count >= self.failure_threshold
        ):
            self._state = CircuitState.OPEN

    def reset(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_calls = 0

    def failure_count(self) -> int:
        return self._failure_count
