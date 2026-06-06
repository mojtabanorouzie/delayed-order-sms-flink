# System Architecture

## Overview

This project is a showcase of Apache Flink streaming patterns across five independent jobs. All jobs read from the same `Orders` Kafka topic (plus `weather-data` for Job 5), apply different stateful logic keyed by different dimensions, and write to separate output topics.

The jobs are deliberately independent — they share no state, no operator graph, and no consumer group. This mirrors how a real delivery platform would evolve: features are added and deployed without touching existing jobs.

---

## Data Flow

```
                    ┌──────────────┐
  Order Simulator   │ Kafka:Orders │   (compacted, keyed by orderId)
  or real system ──▶│  partitions:3│
                    └──────┬───────┘
                           │
          ┌────────────────┼────────────────┬────────────────┐
          │                │                │                │
          ▼                ▼                ▼                ▼
   ┌─────────────┐  ┌─────────────┐  ┌───────────────┐  ┌──────────────────┐
   │ Job 1: SMS  │  │ Job 2:      │  │ Job 3: Courier│  │ Job 4: Restaurant│
   │ keyBy(      │  │ AutoRefund  │  │ Overload      │  │ Bottleneck       │
   │  orderId)   │  │ keyBy(      │  │ keyBy(        │  │ keyBy(storeId)   │
   └──────┬──────┘  │  orderId)   │  │  courierId)   │  └────────┬─────────┘
          │         └──────┬──────┘  └───────┬───────┘           │
          ▼                ▼                 ▼                    ▼
   sms-commands    refund-commands   courier-pause-      restaurant-alerts
                                     commands

                    ┌──────────────┐
  Weather system   │Kafka:weather │   (compacted, keyed by region=zoneId)
  or simulator ───▶│  data        │
                    └──────┬───────┘
                           │
                    ┌──────┴───────────────────────────┐
                    │  Job 5: SurgePricing             │
                    │  keyBy(zoneId)                   │
                    │  CoProcessFunction               │
                    │  (Orders ⋈ weather-data)         │
                    └──────────────────────────────────┘
                                    │
                            surge-pricing-signals

  All jobs ──────────────────────────────────────────▶ dead-letter-events
```

---

## Jobs in Detail

### Job 1 — Delayed Order SMS Detection

**Entry class:** `DelayedOrderSmsJob`
**Consumer group:** `delayed-order-sms-flink`

```
OrderState (from Orders)
  → OrderStateDeserializationFunction  [side: dead-letter]
  → keyBy(orderId)
  → DelayedOrderProcessFunction
       State:  ValueState<OrderState>   (TTL 7d)
               ValueState<Boolean>     delaySmsEmitted (TTL 7d)
       Timer:  processing-time at expectedDeliveryTime
       Logic:  on timer → if active && !emitted → emit SmsCommand, set emitted=true
  → sms-commands
```

**Idempotency key:** `commandId = orderId + ":DELAY_SMS"`

---

### Job 2 — Auto-Refund

**Entry class:** `AutoRefundJob`
**Consumer group:** `auto-refund-flink`

```
OrderState
  → keyBy(orderId)
  → DelayedOrderRefundProcessFunction
       State:  ValueState<Instant>   etaTimerFiredAt (TTL 7d)
               ValueState<Boolean>   refundEmitted   (TTL 7d)
       Timer 1: processing-time at expectedDeliveryTime
       Timer 2: processing-time at expectedDeliveryTime + refundDelayMs
       Logic:  Timer1 fires → register Timer2 if order still active
               Timer2 fires → if still active && !refundEmitted → emit RefundCommand
  → refund-commands
```

The two-timer design means the refund delay is configurable at runtime without recompiling. Setting `--refund.delay.minutes 0` in e2e tests makes Timer2 fire immediately after Timer1.

---

### Job 3 — Courier Overload Detection

**Entry class:** `CourierOverloadJob`
**Consumer group:** `courier-overload-flink`

```
OrderState
  → keyBy(courierId)
  → CourierOverloadProcessFunction
       State:  MapState<orderId, Boolean>  activeOrders (used as set, TTL 7d)
               ValueState<Boolean>         isPaused     (TTL 7d)
       Logic:  terminal status  → remove from map
               non-terminal     → put in map
               count = iterate map values
               count >= overloadThreshold && !paused → emit PAUSE, isPaused=true
               count < resumeThreshold  && paused   → emit RESUME, isPaused=false
  → courier-pause-commands
```

Using `MapState` as a set (value is always `true`) allows O(1) put/remove and prevents double-counting when order status is updated multiple times.

---

### Job 4 — Restaurant Queue Bottleneck

**Entry class:** `RestaurantBottleneckJob`
**Consumer group:** `restaurant-bottleneck-flink`

```
OrderState
  → keyBy(storeId)
  → RestaurantBottleneckProcessFunction
       State:  MapState<orderId, Long>         acceptedOrders  (acceptedAt ms, TTL 7d)
               ValueState<RestaurantWindowState> currentWindow  (TTL 7d)
       Timer:  processing-time at windowEnd = now + windowSizeMs
       Logic:  ACCEPTED  → record acceptedAt; ensure window timer
               PICKED_UP → delta = lastUpdatedAt - acceptedAt; accumulate in window
               onTimer   → avgMs = sumMs / count
                            avgMs > alertThresholdMs → CRITICAL
                            avgMs > baselineMs       → WARNING
                            else                     → no output
  → restaurant-alerts
```

The pickup-time delta is measured using `lastUpdatedAt` on both events, not wall-clock time. This means the simulated 20-minute delta in e2e scenarios is faithfully reproduced even when events arrive within milliseconds of each other.

---

### Job 5 — Dynamic Surge Pricing

**Entry class:** `SurgePricingJob`
**Consumer group:** `surge-pricing-flink` (orders), `surge-pricing-flink-weather` (weather)

```
Orders stream  → keyBy(zoneId)   ─────────────────────────┐
                                                           ▼
                                          DynamicSurgePricingCoProcessFunction
weather-data   → keyBy(region)   ────────▶
                                               State:  ValueState<WeatherData>      currentWeather  (TTL 7d)
                                                       ValueState<SurgeWindowState> demandWindow    (TTL 7d)
                                                       ValueState<Double>           lastMultiplier  (TTL 7d)
                                               Timer:  processing-time at now + windowSizeMs
                                               Logic:  processElement2 (weather) → update currentWeather
                                                       processElement1 (CREATED order)
                                                         → isAtRisk? (ETA remaining > threshold)
                                                         → accumulate in window; register timer
                                                       onTimer
                                                         → demandFactor = atRiskCount / totalOrders
                                                         → weatherFactor = f(condition)
                                                         → combined = 1.0 + (demandFactor × weight) × weatherFactor
                                                         → if combined >= threshold && |Δ| >= changeThreshold → emit
  → surge-pricing-signals
```

**Weather factor table:**

| Condition | Factor |
|---|---|
| CLEAR | 0.95 |
| CLOUDY | 1.00 |
| RAIN | 1.20 |
| SNOW | 1.40 |
| UNKNOWN | 1.00 |

**Change filter:** suppresses signals when the multiplier changed less than `changeThreshold` (default 5%) relative to the last emitted value. Prevents chatty near-identical updates to downstream pricing systems.

---

## Kafka Topics

| Topic | Partitions | Cleanup | Retention | Purpose |
|---|---|---|---|---|
| `Orders` | 3 | compact | indefinite | Full order-state snapshots (latest per key) |
| `weather-data` | 4 | compact | indefinite | Latest weather per zone (latest per key) |
| `sms-commands` | 3 | delete | 7 days | SMS delay commands |
| `refund-commands` | 8 | delete | 7 days | Auto-refund commands |
| `courier-pause-commands` | 8 | delete | 7 days | Courier pause/resume commands |
| `restaurant-alerts` | 4 | delete | 7 days | Restaurant bottleneck ops alerts |
| `surge-pricing-signals` | 8 | delete | 7 days | Zone surge pricing signals |
| `dead-letter-events` | 3 | delete | 30 days | Malformed/invalid events |

Partition counts reflect expected throughput ratios. `surge-pricing-signals` and `refund-commands` have 8 partitions because downstream consumers (pricing engine, payment service) need higher parallelism.

---

## State Design

Every job uses `EmbeddedRocksDBStateBackend` with incremental checkpointing. State TTL is set to 7 days (configurable) using the non-deprecated `StateTtlConfig.newBuilder(Duration.ofDays(n))` API. TTL cleanup runs during full snapshots.

| Job | State | Type | Purpose |
|---|---|---|---|
| SMS | `order-state` | `ValueState<OrderState>` | Current order snapshot |
| SMS | `delay-sms-emitted` | `ValueState<Boolean>` | Idempotency flag |
| Refund | `eta-timer-fired-at` | `ValueState<Instant>` | Records when ETA timer fired |
| Refund | `refund-emitted` | `ValueState<Boolean>` | Idempotency flag |
| Courier | `active-orders` | `MapState<orderId, Boolean>` | Active-order set per courier |
| Courier | `is-paused` | `ValueState<Boolean>` | Current pause state |
| Restaurant | `accepted-orders` | `MapState<orderId, Long>` | Accepted-at ms per order |
| Restaurant | `restaurant-window-state` | `ValueState<RestaurantWindowState>` | Current window accumulator |
| Surge | `zone-weather` | `ValueState<WeatherData>` | Latest weather for zone |
| Surge | `surge-demand-window` | `ValueState<SurgeWindowState>` | Window order counts |
| Surge | `last-emitted-multiplier` | `ValueState<Double>` | For change-threshold filter |

---

## Checkpointing

All jobs use identical checkpoint configuration:

- **Mode:** EXACTLY_ONCE
- **Interval:** 10 seconds
- **Min pause between checkpoints:** 5 seconds
- **Timeout:** 60 seconds
- **Retention:** RETAIN_ON_CANCELLATION (savepoint-compatible recovery)

Incremental RocksDB checkpointing means only changed SST files are written per interval. For jobs with large state (Courier, Restaurant), this significantly reduces checkpoint overhead versus a full snapshot.

---

## Failure and Recovery

On job failure, Flink restarts from the latest completed checkpoint using the fixed-delay restart strategy (3 attempts, 5s between attempts). State is fully restored from RocksDB. Output idempotency keys (`commandId`, `alertId`, `signalId`) ensure downstream consumers can safely deduplicate any replayed output.

See [runbooks/failure-test-runbook.md](runbooks/failure-test-runbook.md) for recovery procedures.

---

## Dead-Letter Routing

Every job routes invalid events to `dead-letter-events` via Flink side outputs. Two DLQ paths exist per job:

1. **Parse failures** — `OrderStateDeserializationFunction.DEAD_LETTER_TAG`: JSON that cannot be deserialized into `OrderState`.
2. **Invalid orders** — each processor's `INVALID_ORDER_TAG`: events that deserialized correctly but fail business validation (null orderId, null status, etc.).

Each `DeadLetterEvent` records the original raw payload, the error reason, the source topic, and the detection timestamp.

See [runbooks/dead-letter-runbook.md](runbooks/dead-letter-runbook.md) for investigation procedures.
