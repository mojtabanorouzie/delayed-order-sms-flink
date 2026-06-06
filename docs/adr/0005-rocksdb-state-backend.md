# ADR-0005: EmbeddedRocksDBStateBackend

**Status:** Accepted
**Date:** 2024-01-01

---

## Context

All five jobs maintain persistent keyed state. The two Flink state backends available are:

- **`HashMapStateBackend`** — state lives entirely in JVM heap
- **`EmbeddedRocksDBStateBackend`** — state is serialized to an embedded RocksDB instance on local disk; checkpoints are written to durable storage

Each job's state characteristics:

| Job | State type | Expected cardinality |
|---|---|---|
| SMS | 2× `ValueState` per orderId | Millions of orders, ~100 bytes each |
| Refund | 2× `ValueState` per orderId | Same as SMS |
| Courier | `MapState<orderId, Boolean>` + `ValueState<Boolean>` per courierId | Thousands of couriers, each with up to N active orders |
| Restaurant | `MapState<orderId, Long>` + `ValueState<WindowState>` per storeId | Thousands of stores |
| Surge | 3× `ValueState` per zoneId | Thousands of zones |

---

## Decision

`EmbeddedRocksDBStateBackend` with incremental checkpointing for all jobs.

---

## Rationale

### Heap state does not scale for order volumes

SMS and Refund jobs hold state for every in-flight order (created but not yet terminal). At millions of orders with a 7-day TTL, the heap state backend would require multi-GB JVM heaps to avoid GC pressure. RocksDB keeps state on disk and serves reads from its block cache — it is designed for this access pattern.

### Courier MapState is unbounded per key

The `active-orders` `MapState` in the courier overload job holds one entry per active order assigned to that courier. A high-volume courier might have 50+ simultaneous active orders. With `HashMapStateBackend`, all courier state must fit in the JVM heap of the task slot. With RocksDB, entries are serialized and stored on disk; only the working set (recently accessed keys) is in memory via the block cache.

### Incremental checkpointing reduces checkpoint time and storage

With `HashMapStateBackend`, every checkpoint writes the full state snapshot. With RocksDB incremental checkpointing, only the SST files that changed since the last checkpoint are uploaded. For steady-state operation (mostly reads, infrequent state mutations), incremental checkpoints are 90–99% smaller than full snapshots. This allows the 10-second checkpoint interval without saturating the checkpoint storage backend.

### TTL cleanup is compatible

Flink's `StateTtlConfig` works with both backends. With RocksDB, compaction-filter-based TTL cleanup runs as part of RocksDB's own compaction, avoiding the need for full-table scans during cleanup. This is especially important for the `MapState` entries in the Courier and Restaurant jobs.

---

## Configuration

```java
RocksDBStateBackend rocksDb = new RocksDBStateBackend(checkpointPath, true); // true = incremental
env.setStateBackend(rocksDb);
```

The `checkpointPath` is set per-environment:
- Local/Docker: `file:///opt/flink/checkpoints`
- Production: `s3://your-bucket/flink-checkpoints` or `gs://...` or `hdfs://...`

---

## Consequences

### Accepted costs

- **Serialization overhead** — RocksDB serializes/deserializes state on every access. For high-throughput operators (thousands of events per second per key), this adds latency compared to heap-based state. At the throughput levels of this system (orders per zone are sparse), this overhead is negligible.
- **Local disk dependency** — RocksDB requires a writable local directory. In containerized environments, this is a bind-mounted or ephemeral volume. If the local disk is lost, the job recovers from the last checkpoint on durable storage — no data loss, but recovery time depends on checkpoint frequency.
- **RocksDB tuning** — Production deployments may need to tune RocksDB's block cache size, write buffer size, and compaction settings for specific throughput patterns. Defaults work for this showcase; see the [scaling runbook](scaling-runbook.md) for guidance.

### What this enables

- Sub-second checkpoint intervals are feasible in production (we use 10 seconds as a conservative default).
- State can grow to hundreds of GB per task slot without heap tuning.
- Recovery time scales with state change rate (incremental diff), not total state size.

---

## When to reconsider

Switch to `HashMapStateBackend` if:
- Total state fits comfortably in heap (< 512 MB per task slot) AND
- Checkpoint latency is not a concern (acceptable to pause processing during full snapshot) AND
- You need predictable single-millisecond state access latency (no serialization round-trip)

This might apply to a small-scale prototype or a stateless-heavy job with minimal keyed state.
