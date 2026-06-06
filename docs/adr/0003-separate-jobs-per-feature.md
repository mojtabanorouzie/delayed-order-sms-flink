# ADR-0003: One Flink Job per Feature

**Status:** Accepted
**Date:** 2024-01-01

---

## Context

The system needs to implement five distinct streaming capabilities:

1. Delayed-order SMS detection
2. Auto-refund after ETA breach
3. Courier overload detection
4. Restaurant queue bottleneck detection
5. Dynamic surge pricing with weather data

These could be implemented as:

- **Option A** — A single Flink job that runs all logic in one operator graph
- **Option B** — One Flink job per feature, each with its own consumer group, state, and deployment lifecycle

---

## Decision

One Flink job per feature (Option B).

---

## Rationale

### Failure isolation

A bug or exception storm in the surge pricing job does not affect SMS delivery. With a monolith, a `NullPointerException` in one operator can trigger the job-level restart strategy and halt unrelated features.

### Independent deployment

Each feature can be updated, restarted from a savepoint, or rolled back without touching the others. The surge pricing job can be paused for tuning while SMS continues uninterrupted.

### Scaling dimensions differ

Each feature has different throughput and state characteristics:
- SMS (Job 1): low-throughput, small state per key, timer-heavy
- Courier overload (Job 3): high-throughput, `MapState` per courier (can be large)
- Surge pricing (Job 5): dual-stream, weather-state per zone

A monolith would be forced to scale to the worst-case job. Separate jobs allow per-feature parallelism (`--parallelism N`).

### Independent consumer offsets

Each job has its own Kafka consumer group. If the surge job falls behind due to a weather data spike, it does not block the SMS job from advancing its own offset. With a shared consumer group, the slowest path limits the entire pipeline.

### Operational clarity

The Flink UI shows five named jobs. An alert on `restaurant-alerts` latency leads directly to the restaurant bottleneck job. In a monolith, debugging requires reading the operator graph to find which subgraph is responsible.

---

## Consequences

### Accepted costs

- **Redundant Kafka consumers** — Each job maintains its own KafkaSource, which means five separate consumer connections to the same `Orders` topic. At this scale this is negligible; in a high-partition cluster, consider a consumer-group-aware fanout service.
- **Operational overhead** — Five jobs to monitor, five savepoints to take before a cluster upgrade. Runbooks address this via the scripted startup in `run_e2e.py`.
- **No shared state** — If two jobs need the same derived fact (e.g., "is this order at-risk?"), each computes it independently. This is currently acceptable; if shared derived state becomes necessary, introduce a dedicated Kafka topic as the intermediary rather than coupling the jobs.

### What this enables

- Each job can be tested in complete isolation using Flink's `OneInputStreamOperatorTestHarness` / `TwoInputStreamOperatorTestHarness` without mocking adjacent logic.
- Job-specific configuration tuning without restart blast radius.
- Future jobs can be added by writing a new `main()` class and submitting it — no existing code touched.
