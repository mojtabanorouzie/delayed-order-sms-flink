# ADR-0001: Processing Time vs Event Time for Delayed Order Detection

**Status:** Accepted

**Date:** 2025-01-15

**Deciders:** Delayed Order SMS Team

## Context

The Delayed Order SMS Flink job must detect when an order's `expectedDeliveryTime` has passed without the order reaching a terminal state (`DELIVERED`, `CANCELLED`). When this occurs, an SMS delay command must be emitted.

Two timer strategies are available in Apache Flink:

1. **Processing-time timers** — fire based on the system clock of the machine running the Flink operator.
2. **Event-time timers** — fire based on watermarks derived from event timestamps in the data stream.

### Trade-offs Considered

| Aspect | Processing Time | Event Time |
|--------|----------------|------------|
| Simplicity | Simple; no watermark strategy needed | Requires watermarks and handling of late data |
| Determinism | Non-deterministic on replay | Deterministic if data is replayed with same timestamps |
| Accuracy | Timer fires close to wall-clock time | Timer fires based on event time, which may lag |
| Late data | Not applicable | Must define allowed lateness and side outputs |
| Operational complexity | Low | Higher — watermark monitoring, idle sources |

### Why Processing Time Was Chosen

- This is a **POC/MVP** — the priority is correctness of the core logic, not production-grade event-time semantics.
- The `expectedDeliveryTime` is a business deadline that maps naturally to wall-clock time. If an order is expected by 2:00 PM, the SMS should go out shortly after 2:00 PM real time.
- Event-time processing would require watermarks, which add complexity (idle partitions, watermark stalls, late data handling) without proportional benefit at this stage.
- The Kafka source is configured with `WatermarkStrategy.noWatermarks()`, explicitly choosing processing time.
- Non-deterministic replay is an acceptable trade-off: in a disaster recovery scenario, replaying events and re-emitting SMS commands is mitigated by the **idempotency key** (`commandId = orderId + ":DELAY_SMS"`) and the downstream SMS service's deduplication.

## Decision

**Use Flink processing-time timers** (via `TimerService.registerProcessingTimeTimer()`) for delayed order detection. The source explicitly uses `WatermarkStrategy.noWatermarks()` to avoid any watermark generation.

## Consequences

### Positive
- Significantly simpler code — no watermark strategy, no allowed lateness, no side outputs for late data.
- Timer fires close to actual wall-clock time, matching user expectations.
- Fewer operational concerns (no watermark monitoring needed).

### Negative
- Non-deterministic on replay: if the job is restored from a savepoint and replays historical data, timers will fire based on current processing time, not original event time. SMS may be re-emitted.
- **Mitigation:** Idempotency keys in `SmsCommand` prevent duplicate SMS delivery at the downstream service level. The `delaySmsEmitted` boolean in Flink state prevents duplicate emission within a single job run.

### Neutral
- Not suitable for use cases requiring exactly-once time-based semantics across restarts. If this becomes a requirement, migrate to event time with watermarks.

## References

- [Flink Documentation: Timers](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/process_function/#timers)
- [Flink Documentation: Generating Watermarks](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/event-time/generating_watermarks/)
- ADR-0002: SMS Idempotency Strategy