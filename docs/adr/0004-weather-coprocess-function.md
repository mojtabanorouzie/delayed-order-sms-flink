# ADR-0004: KeyedCoProcessFunction for Weather Enrichment

**Status:** Accepted
**Date:** 2024-01-01

---

## Context

Job 5 (Dynamic Surge Pricing) needs to combine two streams:

1. **Orders stream** — a stream of `OrderState` events, keyed by `zoneId`
2. **Weather data stream** — a stream of `WeatherData` events representing current conditions per zone/region

The weather data needs to enrich each demand window computation. The key question is: how does the Flink job obtain the current weather for a zone at the time a window fires?

Three approaches were considered:

**Option A — Async I/O enrichment**
Call an external weather API asynchronously per window evaluation. Uses `AsyncFunction` with a timeout.

**Option B — Separate enrichment job → Kafka topic**
A pre-processing job joins weather to order events and emits enriched events to an intermediate topic. The surge pricing job reads enriched events.

**Option C — `KeyedCoProcessFunction`**
Both streams are read by the same Flink job. Weather updates are stored in keyed `ValueState`. Window evaluation reads from this state.

---

## Decision

`KeyedCoProcessFunction` (Option C).

---

## Rationale

### No external dependency at evaluation time

Options A and B introduce a dependency that must be available when the window timer fires. With `KeyedCoProcessFunction`, the weather state is already in RocksDB. The timer callback is entirely self-contained — no I/O, no external call, no timeout path.

### Exactly-once semantics preserved end-to-end

Flink's exactly-once checkpoint protocol covers both the `Orders` source offset and the `weather-data` source offset together. If the job restores from a checkpoint, both streams replay from their committed offsets and the keyed weather state is restored exactly as it was at checkpoint time.

With async I/O (Option A), the async call is not part of the checkpoint. A restored job would re-execute window evaluations with whatever the current weather API returns at recovery time, which may differ from what the pre-failure evaluation used.

### Simplicity

`KeyedCoProcessFunction` is a single class with two `processElement` overrides and one `onTimer`. There is no additional Kafka topic, no additional job, and no HTTP client with retry and circuit-breaker logic.

### Weather data is naturally keyed

The `weather-data` topic is a compacted topic keyed by region (which equals `zoneId`). `keyBy(WeatherData::getRegion)` aligns perfectly with `keyBy(OrderState::getZoneId)`. Flink routes both streams to the same task slot per key, so no network shuffle is required at evaluation time.

### Last-known-value semantics

Weather data is not emitted on a fixed schedule — it updates when conditions change. Storing the last received weather in `ValueState<WeatherData>` means the surge evaluation always uses the most recent known weather for the zone, regardless of when it last arrived. This is exactly the correct semantics for enrichment.

---

## Trade-offs Accepted

### Weather latency before first event

Until the first weather event for a zone arrives, `currentWeather` is `null`. The `onTimer` implementation falls back to `WeatherCondition.UNKNOWN` (factor 1.0) in this case. This means the first window for a zone evaluates without a weather boost. This is acceptable — the zone had no known weather context yet.

### Not suitable for high-cardinality real-time weather

If weather were emitted at very high frequency (e.g., one event per second per zone), this design would absorb significant throughput on the weather sub-stream. For this use case, weather events arrive infrequently (minutes between updates), so this is not a concern.

### Key alignment must be maintained

`WeatherData.region` must equal the `OrderState.zoneId` values it is meant to enrich. This is a soft contract enforced by the simulator and the upstream weather producer — not enforced by the Flink topology itself. A mismatch silently results in zones having no weather state (fallback to UNKNOWN). The DLQ logs any weather events that fail deserialization but not mismatched keys.

---

## Alternatives Reconsidered

**Async I/O (Option A)** would be appropriate if the weather source were a low-latency cache (Redis, Memcached) with sub-millisecond response times. The added exactly-once complexity and external dependency are not worth it when a compacted Kafka topic already serves as the weather source.

**Separate enrichment job (Option B)** adds an intermediate Kafka topic and an additional job to operate. It would only be warranted if multiple downstream jobs needed the same enriched stream. Currently only the surge pricing job needs weather, so the extra infrastructure is not justified.
