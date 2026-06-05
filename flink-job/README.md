# flink-job

Apache Flink streaming job that detects delayed orders and emits idempotent SMS commands.

## What it does

Consumes full order-state snapshots from the compacted `Orders` Kafka topic, maintains per-order keyed state, and registers a processing-time timer at `expectedDeliveryTime`. When the timer fires and the order is still active (not DELIVERED or CANCELLED), it emits one `SEND_DELAY_SMS` command to the `sms-commands` topic.

## Key design points

- **Processing time**: timers fire against wall-clock, not event time — the question is "has real time passed the ETA?" See [ADR-0001](../docs/adr/0001-processing-time-vs-event-time.md).
- **Idempotency**: `commandId = orderId + ":DELAY_SMS"` is deterministic; the `delaySmsEmitted` flag in state prevents re-emission. See [ADR-0002](../docs/adr/0002-sms-idempotency-strategy.md).
- **Stale update rejection**: events with `lastUpdatedAt` older than stored state are silently dropped.
- **Dead-letter routing**: malformed JSON and invalid order states go to `dead-letter-events` via Flink side outputs.
- **State TTL**: configurable (default 7 days) via RocksDB TTL to bound state growth.

## Build

```bash
mvn clean package
# produces: target/delayed-order-sms-flink-job.jar
```

## Configuration

All parameters accept both CLI flags (`--key value`) and environment variables (`KEY_NAME`).

| Parameter | Env var | Default |
|---|---|---|
| `kafka.bootstrap.servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `orders.topic` | `ORDERS_TOPIC` | `Orders` |
| `sms.commands.topic` | `SMS_COMMANDS_TOPIC` | `sms-commands` |
| `dead.letter.topic` | `DEAD_LETTER_TOPIC` | `dead-letter-events` |
| `consumer.group.id` | `CONSUMER_GROUP_ID` | `delayed-order-sms-flink` |
| `checkpoint.storage.path` | `CHECKPOINT_STORAGE_PATH` | `file:///tmp/flink-checkpoints` |
| `checkpoint.interval.ms` | `CHECKPOINT_INTERVAL_MS` | `10000` |
| `parallelism` | `PARALLELISM` | `1` |
| `restart.attempts` | `RESTART_ATTEMPTS` | `3` |
| `restart.delay.ms` | `RESTART_DELAY_MS` | `5000` |
| `state.ttl.days` | `STATE_TTL_DAYS` | `7` |

## Submit to local Flink cluster

```bash
docker exec flink-jobmanager flink run \
  -c com.company.delayedordersms.DelayedOrderSmsJob \
  /opt/flink/usrlib/delayed-order-sms-flink-job.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --orders.topic Orders \
  --sms.commands.topic sms-commands \
  --dead.letter.topic dead-letter-events \
  --consumer.group.id delayed-order-sms-flink \
  --checkpoint.storage.path file:///opt/flink/checkpoints \
  --parallelism 1
```

## Metrics

| Counter | Description |
|---|---|
| `delayed_orders_detected` | Orders whose ETA passed while still active |
| `sms_commands_emitted` | SMS commands written to the sink |
| `stale_updates_ignored` | Events dropped due to older `lastUpdatedAt` |
| `invalid_messages` | Events dropped by the validation check |
| `parse_errors` | Events that failed JSON deserialization |

## Tests

```bash
mvn test
```

Covers: `DelayedOrderProcessFunction` (10 cases), `OrderStateDeserializationFunction`, and `JobConfig` parsing.
