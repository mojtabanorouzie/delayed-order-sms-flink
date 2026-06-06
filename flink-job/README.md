# flink-job

Five Apache Flink streaming jobs packaged as a single Maven module and fat JAR. Each job is an independent `main()` entry point; the specific class is passed to `flink run -c` at submission time.

## Jobs

### Job 1 — DelayedOrderSmsJob

Detects orders that pass their `expectedDeliveryTime` without reaching a terminal state and emits one idempotent `SEND_DELAY_SMS` command per order.

- Pattern: `KeyedProcessFunction`, processing-time timer registered at `expectedDeliveryTime`
- Key: `orderId`
- Output: `sms-commands`

### Job 2 — AutoRefundJob

Issues automatic refund commands for orders that are severely delayed (timer fires `refundDelayMinutes` after `expectedDeliveryTime` and the order is still active).

- Pattern: `KeyedProcessFunction`, two-stage timer (ETA timer → refund-delay timer)
- Key: `orderId`
- Output: `refund-commands`

### Job 3 — CourierOverloadJob

Emits a `COURIER_PAUSE` command when an active-order count for a courier reaches or exceeds `overloadThreshold`; emits `COURIER_RESUME` when it drops below `resumeThreshold`.

- Pattern: `KeyedProcessFunction`, `MapState<orderId, Boolean>` as an active-order set
- Key: `courierId`
- Output: `courier-pause-commands`

### Job 4 — RestaurantBottleneckJob

Measures ACCEPTED→PICKED_UP pickup time per restaurant over a tumbling processing-time window. Emits `CRITICAL` or `WARNING` alerts when average exceeds configurable thresholds.

- Pattern: `KeyedProcessFunction` + manual tumbling window (processing-time timer)
- Key: `storeId`
- Output: `restaurant-alerts`

### Job 5 — SurgePricingJob

Combines per-zone order demand (at-risk orders as a fraction of total orders in a window) with live weather data to compute a surge multiplier. Emits a `SURGE_PRICING` signal when `1.0 + demandFactor × demandWeight × weatherFactor >= surgeThreshold`.

- Pattern: `KeyedCoProcessFunction` joining `Orders` stream (keyed by `zoneId`) with `weather-data` stream (keyed by `region`)
- Key: `zoneId`
- Output: `surge-pricing-signals`

## Build

```bash
# Fat JAR (all dependencies bundled)
mvn clean package -f flink-job/pom.xml

# Skip tests for faster iteration
mvn clean package -f flink-job/pom.xml -DskipTests

# Output: flink-job/target/delayed-order-sms-flink-job.jar
```

## Tests

```bash
mvn test -f flink-job/pom.xml
# Tests run: 100, Failures: 0
```

All processor tests use `KeyedOneInputStreamOperatorTestHarness` (Jobs 1–4) or `KeyedTwoInputStreamOperatorTestHarness` (Job 5). Processing time is controlled via `harness.setProcessingTime()` to fire timers deterministically.

## Configuration

All parameters accept both CLI flags (`--key value`) and environment variables (`KEY_UPPERCASED_WITH_UNDERSCORES`). Environment variables take precedence over CLI flags.

### Shared parameters (all jobs)

| CLI flag | Env var | Default | Description |
|---|---|---|---|
| `kafka.bootstrap.servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `orders.topic` | `ORDERS_TOPIC` | `Orders` | Source topic |
| `dead.letter.topic` | `DEAD_LETTER_TOPIC` | `dead-letter-events` | DLQ topic |
| `checkpoint.storage.path` | `CHECKPOINT_STORAGE_PATH` | `file:///tmp/flink-checkpoints` | Checkpoint directory |
| `checkpoint.interval.ms` | `CHECKPOINT_INTERVAL_MS` | `10000` | Checkpoint interval |
| `parallelism` | `PARALLELISM` | `1` | Flink job parallelism |
| `restart.attempts` | `RESTART_ATTEMPTS` | `3` | Max restart attempts |
| `restart.delay.ms` | `RESTART_DELAY_MS` | `5000` | Delay between restarts |
| `state.ttl.days` | `STATE_TTL_DAYS` | `7` | State TTL for RocksDB cleanup |

### Job 1 — DelayedOrderSmsJob

| CLI flag | Default |
|---|---|
| `sms.commands.topic` | `sms-commands` |
| `consumer.group.id` | `delayed-order-sms-flink` |

### Job 2 — AutoRefundJob

| CLI flag | Default | Notes |
|---|---|---|
| `refund.commands.topic` | `refund-commands` | |
| `consumer.group.id` | `auto-refund-flink` | |
| `refund.delay.minutes` | `30` | Minutes after ETA before refund fires |

### Job 3 — CourierOverloadJob

| CLI flag | Default | Notes |
|---|---|---|
| `courier.commands.topic` | `courier-pause-commands` | |
| `consumer.group.id` | `courier-overload-flink` | |
| `overload.threshold` | `8` | Active orders to trigger PAUSE |
| `resume.threshold` | `5` | Active orders to trigger RESUME |
| `pause.enabled` | `true` | Feature flag |

### Job 4 — RestaurantBottleneckJob

| CLI flag | Default | Notes |
|---|---|---|
| `restaurant.alerts.topic` | `restaurant-alerts` | |
| `consumer.group.id` | `restaurant-bottleneck-flink` | |
| `window.size.seconds` | `300` | Tumbling window duration |
| `alert.threshold.minutes` | `15` | Avg pickup > this → CRITICAL |
| `baseline.minutes` | `8` | Avg pickup > this → WARNING |
| `alert.enabled` | `true` | Feature flag |

### Job 5 — SurgePricingJob

| CLI flag | Default | Notes |
|---|---|---|
| `surge.signals.topic` | `surge-pricing-signals` | |
| `weather.topic` | `weather-data` | Weather source topic |
| `consumer.group.id` | `surge-pricing-flink` | |
| `window.size.seconds` | `60` | Demand measurement window |
| `at.risk.threshold.minutes` | `25` | ETA remaining > this → order is at-risk |
| `surge.threshold` | `1.15` | Minimum multiplier to emit signal |
| `demand.weight` | `0.5` | Weight of demand component |
| `change.threshold` | `0.05` | Suppress signal if multiplier changed < 5% |
| `max.multiplier` | `3.0` | Hard cap on surge multiplier |
| `surge.enabled` | `true` | Feature flag |

## Metrics

Each processor exposes custom Flink metrics (visible in the Flink UI under the job's operator metrics).

| Job | Metric | Description |
|-----|--------|-------------|
| SMS | `delayed_orders_detected` | Orders whose ETA passed while active |
| SMS | `sms_commands_emitted` | SMS commands written |
| SMS | `stale_updates_ignored` | Events dropped for stale `lastUpdatedAt` |
| Refund | `refunds_emitted` | Refund commands written |
| Courier | `courier_pauses_emitted` | PAUSE commands written |
| Courier | `courier_resumes_emitted` | RESUME commands written |
| Restaurant | `restaurant_alerts_emitted` | Bottleneck alerts written |
| Surge | `surge_signals_emitted` | Surge pricing signals written |
| Surge | `surge_weather_updates` | Weather state updates received |
| All | `*_invalid_messages` | Events routed to dead-letter |
| All | `*_parse_errors` | JSON deserialization failures |

## Package Structure

```text
src/main/java/com/company/delayedordersms/
├── DelayedOrderSmsJob.java
├── AutoRefundJob.java
├── CourierOverloadJob.java
├── RestaurantBottleneckJob.java
├── SurgePricingJob.java
├── config/
│   ├── DelayedOrderSmsJobConfig.java
│   ├── AutoRefundJobConfig.java
│   ├── CourierOverloadJobConfig.java
│   ├── RestaurantBottleneckJobConfig.java
│   └── SurgePricingJobConfig.java
├── model/
│   ├── OrderState.java          ← Input: order state snapshots
│   ├── OrderStatus.java         ← Enum: CREATED/ACCEPTED/PICKED_UP/DELIVERED/CANCELLED
│   ├── WeatherData.java         ← Input: weather per zone
│   ├── WeatherCondition.java    ← Enum: CLEAR/CLOUDY/RAIN/SNOW/UNKNOWN
│   ├── SmsCommand.java          ← Output: Job 1
│   ├── RefundCommand.java       ← Output: Job 2
│   ├── CourierCommand.java      ← Output: Job 3
│   ├── OpsAlert.java            ← Output: Job 4
│   ├── SurgePricingSignal.java  ← Output: Job 5
│   ├── DeadLetterEvent.java     ← Output: all jobs (DLQ)
│   ├── RestaurantWindowState.java ← Internal state: Job 4
│   └── SurgeWindowState.java    ← Internal state: Job 5
├── processor/
│   ├── DelayedOrderProcessFunction.java
│   ├── DelayedOrderRefundProcessFunction.java
│   ├── CourierOverloadProcessFunction.java
│   ├── RestaurantBottleneckProcessFunction.java
│   └── DynamicSurgePricingCoProcessFunction.java
└── serde/
    ├── OrderStateDeserializationFunction.java
    ├── WeatherDataDeserializationFunction.java
    ├── SmsCommandSerializationSchema.java
    ├── RefundCommandSerializationSchema.java
    ├── CourierCommandSerializationSchema.java
    ├── OpsAlertSerializationSchema.java
    ├── SurgePricingSignalSerializationSchema.java
    └── DeadLetterEventSerializationSchema.java
```
