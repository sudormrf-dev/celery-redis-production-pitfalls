"""Decision tree: asyncio.create_task vs BackgroundTasks vs Celery.

For each real-world scenario this module recommends the right concurrency
primitive and explains the reasoning in concrete terms.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Approach(str, Enum):
    """The three main async-work primitives available in a Python web stack."""

    ASYNCIO_CREATE_TASK = "asyncio.create_task"
    BACKGROUND_TASKS = "FastAPI BackgroundTasks"
    CELERY = "Celery + Redis"


@dataclass
class Scenario:
    """A real-world task scenario with its recommended approach."""

    name: str
    description: str
    recommended: Approach
    rationale: str
    constraints: list[str]
    anti_patterns: list[str]

    def print_recommendation(self) -> None:
        """Pretty-print the recommendation."""
        width = 60
        print(f"\n{'─' * width}")
        print(f"  Scenario : {self.name}")
        print(f"  Task     : {self.description}")
        print(f"  ✔ Use    : {self.recommended.value}")
        print(f"  Why      : {self.rationale}")
        if self.constraints:
            print("  Needs    :")
            for c in self.constraints:
                print(f"             • {c}")
        if self.anti_patterns:
            print("  Avoid    :")
            for ap in self.anti_patterns:
                print(f"             ✗ {ap}")


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------

def decide(
    duration_ms: int,
    survives_restart: bool,
    needs_retry: bool,
    distributed: bool,
    rate_limited: bool,
    in_async_context: bool,
) -> tuple[Approach, str]:
    """Return the recommended approach and a one-line justification.

    Args:
        duration_ms: Expected task duration in milliseconds.
        survives_restart: Must the task survive a process restart?
        needs_retry: Should failures be automatically retried?
        distributed: Must work be spread across multiple machines?
        rate_limited: Does the task need rate-limiting against an external API?
        in_async_context: Is the caller inside an async event loop?

    Returns:
        A tuple of (Approach, rationale string).
    """
    if survives_restart or needs_retry or distributed or rate_limited:
        return (
            Approach.CELERY,
            "Durability / retry / distribution / rate-limiting require a proper broker",
        )
    if duration_ms > 5_000:
        return (
            Approach.CELERY,
            f"Tasks > 5 s ({duration_ms} ms) should not block the web process",
        )
    if in_async_context and duration_ms < 500:
        return (
            Approach.ASYNCIO_CREATE_TASK,
            "Sub-500 ms I/O in an async handler — fire-and-forget in the same loop",
        )
    return (
        Approach.BACKGROUND_TASKS,
        "Short, non-critical work tied to an HTTP response lifecycle",
    )


# ---------------------------------------------------------------------------
# Concrete scenarios
# ---------------------------------------------------------------------------

SCENARIOS: list[Scenario] = [
    Scenario(
        name="Thumbnail generation",
        description="Resize uploaded image to 3 sizes; store in S3 (~3 s)",
        recommended=Approach.CELERY,
        rationale="CPU-bound, > 1 s, must survive worker restart and retry on S3 errors",
        constraints=[
            "time_limit=60s, soft_time_limit=50s",
            "max_retries=3, exponential backoff",
            "Route to 'images' queue on dedicated CPU workers",
        ],
        anti_patterns=[
            "asyncio.create_task — blocks event loop with CPU work",
            "BackgroundTasks — lost on process restart",
        ],
    ),
    Scenario(
        name="Transactional email",
        description="Send order-confirmation email via SendGrid after checkout",
        recommended=Approach.CELERY,
        rationale="Must be delivered exactly once, retried on SMTP errors, rate-limited (< 100/s)",
        constraints=[
            "rate_limit='100/s'",
            "max_retries=5, jitter backoff to avoid thundering herd",
            "ack_late=True so message requeued on worker crash",
        ],
        anti_patterns=[
            "BackgroundTasks — email lost if process exits before send",
            "asyncio.create_task — no retry, no persistence",
        ],
    ),
    Scenario(
        name="Real-time activity counter",
        description="Increment a 'viewed' counter in Redis when a page is loaded (~1 ms)",
        recommended=Approach.ASYNCIO_CREATE_TASK,
        rationale="Pure async I/O, sub-millisecond, no durability needed — broker overhead is wasteful",
        constraints=[
            "Must be inside an async request handler (FastAPI, aiohttp, etc.)",
            "Accept occasional loss on process exit (counters are approximate)",
        ],
        anti_patterns=[
            "Celery — adds 10–50 ms broker round-trip for a 1 ms operation",
            "BackgroundTasks — correct but unnecessarily tied to response lifecycle",
        ],
    ),
    Scenario(
        name="Heavy ML inference",
        description="Run a 10 GB model inference on uploaded audio (~45 s)",
        recommended=Approach.CELERY,
        rationale="Long-running, GPU-bound, needs separate worker fleet with dedicated queue",
        constraints=[
            "time_limit=120s, soft_time_limit=110s",
            "Dedicated 'ml-gpu' queue, concurrency=1 per GPU",
            "max_retries=2 (inference is expensive)",
            "result_expires=3600 — caller polls for result",
        ],
        anti_patterns=[
            "asyncio.create_task — starves event loop for 45 s",
            "BackgroundTasks — ties up web worker, no result propagation",
        ],
    ),
    Scenario(
        name="Request audit log",
        description="Write an access-log row to PostgreSQL after each API call (~5 ms)",
        recommended=Approach.BACKGROUND_TASKS,
        rationale="Cheap DB write, tied to request lifecycle, OK to lose on restart — no broker needed",
        constraints=[
            "Use FastAPI BackgroundTasks (or Starlette equivalent)",
            "Keep it truly fire-and-forget; do NOT await the result",
        ],
        anti_patterns=[
            "Celery — serialization + broker round-trip costs more than the write itself",
            "asyncio.create_task — leaks tasks if loop closes before completion",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Interactive decision tree
# ---------------------------------------------------------------------------

def run_decision_tree(scenario_params: list[dict[str, Any]]) -> None:
    """Walk through the decision tree for a list of parameter dicts."""
    print("\n" + "=" * 60)
    print("  Decision Tree: asyncio vs BackgroundTasks vs Celery")
    print("=" * 60)

    headers = ["Scenario", "Duration", "Survive?", "Retry?", "Distributed?", "Recommended"]
    rows: list[tuple[str, ...]] = []

    for params in scenario_params:
        approach, reason = decide(**{k: v for k, v in params.items() if k != "label"})
        rows.append((
            params["label"],
            f"{params['duration_ms']} ms",
            "yes" if params["survives_restart"] else "no",
            "yes" if params["needs_retry"] else "no",
            "yes" if params["distributed"] else "no",
            approach.value,
        ))
        print(f"\n  [{params['label']}]")
        print(f"    → {approach.value}")
        print(f"    {reason}")

    print("\n" + "─" * 60)
    col_w = [max(len(h), max(len(r[i]) for r in rows)) + 2 for i, h in enumerate(headers)]
    header_line = "".join(h.ljust(col_w[i]) for i, h in enumerate(headers))
    print("  " + header_line)
    print("  " + "-" * sum(col_w))
    for row in rows:
        print("  " + "".join(cell.ljust(col_w[i]) for i, cell in enumerate(row)))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run scenario recommendations and decision tree."""
    print("=" * 60)
    print("  Celery + Redis Production Pitfalls — Decision Tree Demo")
    print("=" * 60)

    # Detailed scenario cards
    for scenario in SCENARIOS:
        scenario.print_recommendation()

    # Decision tree table for quick comparison
    run_decision_tree([
        {
            "label": "Image resize",
            "duration_ms": 3_000,
            "survives_restart": True,
            "needs_retry": True,
            "distributed": False,
            "rate_limited": False,
            "in_async_context": False,
        },
        {
            "label": "Send email",
            "duration_ms": 800,
            "survives_restart": True,
            "needs_retry": True,
            "distributed": False,
            "rate_limited": True,
            "in_async_context": False,
        },
        {
            "label": "Redis counter",
            "duration_ms": 1,
            "survives_restart": False,
            "needs_retry": False,
            "distributed": False,
            "rate_limited": False,
            "in_async_context": True,
        },
        {
            "label": "ML inference",
            "duration_ms": 45_000,
            "survives_restart": True,
            "needs_retry": True,
            "distributed": True,
            "rate_limited": False,
            "in_async_context": False,
        },
        {
            "label": "Audit log",
            "duration_ms": 5,
            "survives_restart": False,
            "needs_retry": False,
            "distributed": False,
            "rate_limited": False,
            "in_async_context": False,
        },
    ])

    print("\n" + "=" * 60)
    print("  Decision tree complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
